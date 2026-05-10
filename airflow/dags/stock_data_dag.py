from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# --- CẤU HÌNH DAG ---

default_args = {
    "owner": "namlap",
    "start_date": datetime(2026, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DOCKER_NETWORK = Variable.get("your_network_name", default_var="your-network")
IMAGE_TAG = Variable.get("stock_image_tag", default_var="latest")

INGESTION_ENV = {
    "STOCK_SYMBOLS": os.getenv("STOCK_SYMBOLS"),
    "PAGE": os.getenv("PAGE", "100"),
    "MAX_THREADS": os.getenv("MAX_THREADS", "4"),
    "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT"),
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"), 
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "MINIO_BUCKET": os.getenv("MINIO_BUCKET"),
    "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
    "KAFKA_TOPIC_MAIN": os.getenv("KAFKA_TOPIC_MAIN"),
    "KAFKA_TOPIC_DLQ": os.getenv("KAFKA_TOPIC_DLQ")
}

DBT_ENV = {
    # dbt báo thiếu 'DBT_USER', ta lấy từ 'DB_USER' trong .env
    "DBT_USER": os.getenv("DB_USER"),
    "DBT_PASSWORD": os.getenv("DB_PASSWORD"),
    "DBT_DATABASE": os.getenv("DB_NAME"),
    "DBT_SCHEMA": os.getenv("DB_SCHEMA"),
    "DBT_HOST": "postgres-dw",
    "DBT_PORT": "5432",
}
with DAG(
    dag_id="stock_pipeline_v4_clean",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["production", "dbt", "cleaned"],
    template_searchpath=['/opt/airflow/sql']
) as dag:

    # 🟡 0. Khởi tạo Database
    init_db = PostgresOperator(
        task_id="init_raw_db",
        postgres_conn_id="postgres_stock",
        sql="create_table.sql"
    )

    # 🟢 1. Ingestion Layer (Docker)
    ingestion_to_kafka = DockerOperator(
        task_id="in_ram_ingestion",
        image=f"stock_ingestion_in_ram:{IMAGE_TAG}",
        command="python main.py",
        network_mode=DOCKER_NETWORK,
        auto_remove='force',
        mount_tmp_dir=False,
        environment=INGESTION_ENV,
    )

    # 🟣 2. Transformation Layer (dbt)
    dbt_transform = DockerOperator(
        task_id="dbt_transform",
        image=f"stock_dbt:{IMAGE_TAG}",
        network_mode=DOCKER_NETWORK,
        auto_remove='force',
        mount_tmp_dir=False,
        environment=DBT_ENV,
        force_pull=False, 
    )
    
    # 🔴 3. Alert Layer (Chỉ chạy khi có lỗi)
    task_alert = TelegramOperator(
        task_id="alert_telegram",
        token=Variable.get("telegram_token", default_var="YOUR_BOT_TOKEN_HERE"),
        chat_id=Variable.get("telegram_chat_id", default_var="YOUR_CHAT_ID_HERE"),
        text="🚨 Pipeline thất bại tại task: {{ dag_run.get_task_instances(state='failed')[0].task_id }}",
        trigger_rule=TriggerRule.ONE_FAILED # Chạy nếu có ít nhất 1 task phía trước fail
    )

    # Task kết thúc
    finished = EmptyOperator(
        task_id="finished",
        trigger_rule=TriggerRule.ALL_DONE # Luôn chạy để đóng flow
    )

    # 🔗 THIẾT LẬP DÒNG CHẢY (FLOW)
    # Flow chính:
    init_db >> ingestion_to_kafka >> dbt_transform >> finished

    # Flow cảnh báo:
    # Nếu bất kỳ task nào trong 3 task chính bị lỗi, Telegram sẽ bắn tin.
    [init_db, ingestion_to_kafka, dbt_transform] >> task_alert >> finished