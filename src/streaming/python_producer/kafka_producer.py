import json
from src.utils.logger import setup_logger
from confluent_kafka import Producer
from src.utils.logger import setup_logger
import pandas as pd
# Thiết lập logger (tuỳ chọn)

logger = setup_logger(
    logger_name="Kafka-producer",
    sub_dir="/app/logging",
    log_file="application.log",
    level=10
)


KAFKA_CONFIG = {
    'bootstrap.servers': 'kafka:9092',

    'acks': 'all',                      # An toàn tuyệt đối
    'enable.idempotence': True,         # Không duplicate
    'retries': 3,                     
    'max.in.flight.requests.per.connection': 5,

    'linger.ms': 10,                    # Batch nhiều hơn
    'batch.size': 65536,                # 64KB
    'compression.type': 'snappy',       # Nhanh + nhẹ CPU

    'message.timeout.ms': 120000,
    'request.timeout.ms': 30000,

    }
producer = Producer(KAFKA_CONFIG)

def delivery_report(err, msg):
    """Callback function: Được gọi khi tin nhắn gửi thành công hoặc thất bại"""
    if err is not None:
        logger.error(f"❌ Gửi thất bại: {err}")
    else:
        logger.debug(f"✅ Đã gửi tới {msg.topic()} [{msg.partition()}]")

def send_to_kafka(df_page: pd.DataFrame, topic_name: str):
    if df_page.empty:
        logger.warning(f"DataFrame trống, bỏ qua việc đẩy vào topic {topic_name}")
        return

    records = df_page.to_dict(orient="records")
    
    for record in records:
        json_data = json.dumps(record).encode('utf-8')
        # Sử dụng 'Ngay' làm key để đảm bảo các record cùng ngày vào cùng 1 partition
        message_key = str(record.get('Ngay', 'unknown')).encode('utf-8')

        producer.produce(
            topic=topic_name,
            value=json_data,
            key=message_key, 
            callback=delivery_report
        )
        # Khuyến nghị: Gọi poll(0) liên tục để giải phóng các callback đã hoàn thành khỏi RAM
        producer.poll(0) 

    logger.info("Đang flush buffer của Kafka Producer...")
    producer.flush() 
    
    logger.info(f"✅ Đã gửi thành công {len(records)} records vào topic '{topic_name}'")