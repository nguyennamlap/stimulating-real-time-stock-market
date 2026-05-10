import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from check_driver import check_spark_driver
from check_stream import get_kafka_lag

def run_system_health_check():
    print("--- 🛡️ BẮT ĐẦU KIỂM TRA HỆ THỐNG ---")

    # 1. Kiểm tra "Sự sống" của Spark App trước
    spark_status = check_spark_driver()
    
    if spark_status == 'alert_telegram':
        print("❌ Spark App đang dừng hoặc lỗi kết nối Master.")
        return "CRITICAL: SPARK_DOWN"

    # 2. Nếu Spark ổn định, tiến hành kiểm tra độ trễ dữ liệu (Lag)
    
    topic = os.getenv("KAFKA_TOPIC", "your_topic")
    group = os.getenv("KAFKA_GROUP", "your_group")
    
    total_lag = get_kafka_lag(topic, group)
    print(f"\n📊 Tổng Lag ghi nhận: {total_lag}")

    # 3. Logic điều hướng hành động
    if total_lag > 10000:
        return "ALERT: TELEGRAM_CRITICAL_LAG"
    
    elif total_lag > 3000:
        return "ACTION: SCALE_UP_SPARK_WORKERS"
    
    elif total_lag > 0:
        return "ACTION: MAINTENANCE_ROUTINE"

    return "STATUS: HEALTHY"

if __name__ == "__main__":
    final_decision = run_system_health_check()
    print(f"\n🚀 QUYẾT ĐỊNH CUỐI CÙNG: {final_decision}")