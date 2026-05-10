# Sử dụng Python slim image để tối ưu dung lượng
FROM python:3.10-slim

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Cài đặt các thư viện hệ thống cơ bản cần thiết cho quá trình build (ví dụ thư viện C cho Pandas/Kafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Chuyển các file requirements của từng module vào container
# Cách này giúp tận dụng Docker cache tốt hơn nếu chỉ 1 module thay đổi requirement
COPY src/ingestion/requirements.txt /app/req_ingestion.txt
COPY src/quality_control/requirements.txt /app/req_qc.txt
COPY src/streaming/python_producer/requirements.txt /app/req_kafka.txt

# Cài đặt dependencies
RUN pip install --no-cache-dir -r /app/req_ingestion.txt \
    && pip install --no-cache-dir -r /app/req_qc.txt \
    && pip install --no-cache-dir -r /app/req_kafka.txt \
    && pip install --no-cache-dir python-dotenv

# Copy toàn bộ mã nguồn vào thư mục làm việc
COPY src/test/main.py /app/main.py
COPY src/utils /app/src/utils
COPY src/ingestion /app/src/ingestion
COPY src/quality_control /app/src/quality_control
COPY src/streaming/python_producer /app/src/streaming/python_producer
# Biến môi trường quan trọng: Giúp Python hiểu thư mục /app chứa các package (src.*)
ENV PYTHONPATH=/app

# # Lệnh mặc định khi chạy container
# CMD ["python", "src/test/main.py"]