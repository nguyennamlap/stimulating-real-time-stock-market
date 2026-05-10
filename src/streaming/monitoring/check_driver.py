import requests

# Hàm kiểm tra spark-master có hoạt động tốt ko, nếu chạy client có thể skip
def check_spark_driver():
    APP_NAME = "PostgresSparkApp" 
    SPARK_MASTER_URL = "http://spark-master:8080/json/" 

    try:
        response = requests.get(SPARK_MASTER_URL, timeout=10)
        response.raise_for_status() # Tự động raise lỗi nếu status_code != 200
        
        data = response.json()
        
        # Tìm app của bạn trong danh sách các ứng dụng đang chạy (activeapps)
        active_apps = [app for app in data.get('activeapps', []) if app['name'] == APP_NAME]
        
        if len(active_apps) > 0:
            # Nếu tìm thấy app và trạng thái là RUNNING
            print(f"App {APP_NAME} is healthy.")
            return 'check_kafka_lag'
        else:
            print(f"App {APP_NAME} not found in active apps!")
            return 'alert_telegram'
            
    except requests.exceptions.RequestException as e:
        print(f"Connection Error: {e}")
        return 'alert_telegram'
    except Exception as e:
        print(f"Unexpected Error: {e}")
        return 'alert_telegram'