import sys
from pyspark.sql.functions import regexp_extract
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
import os
from pyspark.sql.functions import to_date
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType,
    TimestampType, DateType   
)
from pyspark.sql.functions import to_timestamp
from typing import Optional, Dict
from src.utils.logger import setup_logger
from logic.technical_indicators import add_technical_indicators
# 1. Khởi tạo Logger
logger = setup_logger(
    logger_name="cleaner",
    sub_dir="/app/logging",
    log_file="data_cleaner.log",
    level=10 
)

from dotenv import load_dotenv
load_dotenv()

# Hàm config
def create_spark_session(
    app_name,
    postgres_url,
    postgres_user,
    postgres_password,
    postgres_driver="org.postgresql.Driver",
    jar_package: Optional[str] = None,
    extra_configs: Optional[Dict[str, str]] = None
) -> SparkSession:

    builder = SparkSession.builder \
            .appName(app_name) 
  
    if jar_package:
        builder = builder.config("spark.jars.packages", jar_package)

    # extra configs
    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()

    # lưu config (optional)
    spark.conf.set("spark.postgres.url", postgres_url)
    spark.conf.set("spark.postgres.user", postgres_user)
    spark.conf.set("spark.postgres.password", postgres_password)
    spark.conf.set("spark.postgres.driver", postgres_driver)

    return spark


def create_initial_dataframe(spark_session):
    try:
        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        kafka_topic = os.getenv("KAFKA_TOPIC_MAIN")

        # Định nghĩa schema cho JSON
        schema = StructType([
            StructField("Ngay", StringType(), True),          # ban đầu đọc dạng string
            StructField("GiaDieuChinh", DoubleType(), True),
            StructField("GiaDongCua", StringType(), True),
            StructField("ThayDoi", StringType(), True),       # "0.2(0.40 %)" cần xử lý thêm
            StructField("KhoiLuongKhopLenh", StringType(), True),
            StructField("GiaTriKhopLenh", StringType(), True),
            StructField("KLThoaThuan", StringType(), True),
            StructField("GtThoaThuan", StringType(), True),
            StructField("GiaMoCua", StringType(), True),
            StructField("GiaCaoNhat", StringType(), True),
            StructField("GiaThapNhat", StringType(), True),
            StructField("code", StringType(), True)
        ])
        #  Đọc Kafka
        df = (spark_session.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "earliest")
            .load())
        #  Parse JSON
        df = df.selectExpr("CAST(value AS STRING) as raw") \
               .withColumn("jsonData", from_json(col("raw"), schema)) \
               .select("jsonData.*")
        
        #  Chuyển đổi kiểu dữ liệu
        df = df.withColumn("ngay_ts", to_timestamp(col("Ngay"), "dd/MM/yyyy")) \
            .withColumn("giadieuchinh", col("GiaDieuChinh").cast("double")) \
            .withColumn("giadongcua", col("GiaDongCua").cast("double")) \
            .withColumn("khoiluongkhoplenh", col("KhoiLuongKhopLenh").cast("long")) \
            .withColumn("giatrikhoplenh", col("GiaTriKhopLenh").cast("double")) \
            .withColumn("klthoathuan", col("KLThoaThuan").cast("long")) \
            .withColumn("gtthoathuan", col("GtThoaThuan").cast("double")) \
            .withColumn("giamocua", col("GiaMoCua").cast("double")) \
            .withColumn("giacaonhat", col("GiaCaoNhat").cast("double")) \
            .withColumn("giathapnhat", col("GiaThapNhat").cast("double")) \
            .withColumn("thaydoi", regexp_extract(col("ThayDoi"), r"([-+]?[0-9.]+)", 1).cast("double")) \
            .withColumn("code", col("code"))

        # Lọc key
        # Chấp nhận dữ liệu đến trễ tối đa 10 ngày

        df = df.filter(col("code").isNotNull() & col("ngay_ts").isNotNull())

        # vào database rồi xóa dữ liệu trùng lặp bằng primery key, xóa ở đây sẽ bị stateful operation
        df = df.withWatermark("ngay_ts", "10 days") 
        
        df.printSchema()
        logger.info(" Initial streaming DataFrame schema created successfully (JSON).")
        return df

    except Exception as e:
        # Bắt lỗi 
        logger.error(f" Couldn't create initial DataFrame: {e}")
        return None


def process_and_sink(df_batch, batch_id):

    if not df_batch.head(1):
        return
    
    try:
        logger.info(f"Processing batch {batch_id}")

        processed_df = add_technical_indicators(df_batch)

        jdbc_url = os.getenv("POSTGRES_URL")

        db_properties = {
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "driver": "org.postgresql.Driver"
        }

        schema = os.getenv("DB_SCHEMA")
        table = os.getenv("DB_TABLE")
        # schema.table
        processed_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"{schema}.{table}") \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("driver", db_properties["driver"]) \
            .option("batchsize", 1000) \
            .mode("append") \
            .save()
        
        processed_df.unpersist() 
        logger.info(f"Batch {batch_id} written successfully.")

    except Exception as e:
        logger.error(f"Error batch {batch_id}: {e}")


def write_to_postgres(df):
    try:
        checkpoint_dir = os.getenv("CHECKPOINT_DIR", "/tmp/spark_checkpoints/csdl_anhlap")

        query = (df.writeStream
                 .foreachBatch(process_and_sink) # Sử dụng hàm xử lý tùy biến
                 .option("checkpointLocation", checkpoint_dir)
                 .trigger(processingTime='10 seconds')
                 .start())

        logger.info("Streaming query started with foreachBatch.")
        query.awaitTermination()
    except Exception as e:
        logger.error(f"Streaming failed: {e}")
        raise

def write_streaming_data():

    postgres_jar = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.kafka:kafka-clients:3.5.0",
    "org.postgresql:postgresql:42.7.3",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262" ])

    spark = create_spark_session(
        app_name="PostgresSparkApp",
        postgres_url=os.getenv("POSTGRES_URL"),
        postgres_user=os.getenv("DB_USER"),          # Bổ sung
        postgres_password=os.getenv("DB_PASSWORD"),  # Bổ sung
        jar_package=postgres_jar                     
    )

    endpoint_url=os.getenv('MINIO_ENDPOINT')
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')

    hadoop_conf = spark._jsc.hadoopConfiguration()

    hadoop_conf.set("fs.s3a.endpoint", endpoint_url)
    hadoop_conf.set("fs.s3a.access.key", aws_access_key_id)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret_access_key)

    # cực quan trọng
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    df = create_initial_dataframe(spark)

    if df is not None:
        write_to_postgres(df)

if __name__ == "__main__":
    write_streaming_data()