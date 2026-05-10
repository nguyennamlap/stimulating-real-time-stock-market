from confluent_kafka import Consumer, TopicPartition
import os

def get_load_mode(topic_name):
    conf = {
        'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        'group.id': 'check-group-temp', # Nên dùng group ID tạm thời
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    
    try:
        # Lấy danh sách các partition của topic
        metadata = consumer.list_topics(topic_name, timeout=10)
        if topic_name not in metadata.topics:
            print(f"⚠️ Topic '{topic_name}' chưa tồn tại. Giả định là Full Load.")
            return 'full'

        partitions = metadata.topics[topic_name].partitions.keys()
        
        has_data = False
        for p in partitions:
            tp = TopicPartition(topic_name, p)
            low, high = consumer.get_watermark_offsets(tp, timeout=5.0)
            if high > low:
                has_data = True
                break
        
        if has_data:
            print(f"✅ Topic '{topic_name}' đã có dữ liệu. Chuyển sang Incremental Load.")
            return 'incremental'
        else:
            print(f"❌ Topic '{topic_name}' trống. Chạy Full Load.")
            return 'full'
            
    except Exception as e:
        print(f"🚨 Lỗi khi kiểm tra Kafka: {e}")
        return 'full' # Nếu lỗi, an toàn nhất là chạy Full load
    finally:
        consumer.close()


def get_kafka_lag(topic, group_id):
    conf = {
        'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False # Tránh việc check lag làm nhảy offset của group thật
    }
    consumer = Consumer(conf)

    try:
        metadata = consumer.list_topics(topic, timeout=10)
        if topic not in metadata.topics:
            print(f"⚠️ Topic '{topic}' không tồn tại.")
            return 0

        partitions = [TopicPartition(topic, p) for p in metadata.topics[topic].partitions.keys()]
        
        # TỐI ƯU: Lấy tất cả committed offsets trong 1 lần gọi (Batch request)
        committed = consumer.committed(partitions, timeout=10)
        
        total_lag = 0
        for tp in committed:
            # Lấy watermark (low, high) cho từng partition
            low, high = consumer.get_watermark_offsets(tp, timeout=5)
            
            # Xử lý trường hợp offset chưa từng được commit (thường trả về -1001)
            committed_offset = tp.offset if tp.offset >= 0 else low
            
            lag = max(high - committed_offset, 0)
            total_lag += lag
            
            print(f"📍 P{tp.partition} | High: {high} | Committed: {committed_offset} | Lag: {lag}")

        return total_lag

    except KafkaException as e:
        print(f"🚨 Lỗi kết nối Kafka: {e}")
        return 0
    finally:
        consumer.close()

# Hàm 3: LOGIC ĐIỀU PHỐI (Refactored)
def check_kafka_lag_logic():
    # Giả sử lấy thông tin từ env hoặc config
    TOPIC = os.getenv("KAFKA_TOPIC", "your_topic")
    GROUP = os.getenv("KAFKA_GROUP", "your_group")
    
    total_lag = get_kafka_lag(TOPIC, GROUP)
    print(f"\n📊 --- THỐNG KÊ TỔNG LAG: {total_lag} ---")

    # Sử dụng Dictionary Mapping hoặc If-Else để điều hướng
    if total_lag > 10000:
        print("🔥 Nguy cấp! Gửi Alert Telegram...")
        return "alert_telegram"
    
    if total_lag > 3000:
        print("⚡ Khối lượng dữ liệu lớn. Tăng scale Spark Cluster...")
        return "scale_spark"
    
    if total_lag > 0:
        print("🧹 Lag thấp. Chạy bảo trì/Compaction định kỳ...")
        return "spark_maintenance_compaction"
    
    print("✅ Hệ thống ổn định, không có lag.")
    return "idle"