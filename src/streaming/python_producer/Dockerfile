# Sử dụng image Python chính thức (bản slim để tối ưu dung lượng)
FROM python:3.11-slim

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Cài đặt các thư viện hệ thống cần thiết cho librdkafka và pandas
RUN apt-get update && apt-get install -y \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy file requirements vào và cài đặt thư viện Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code vào container
COPY src/streaming/kafka-producer /app/streaming/kafka-producer

# Thiết lập biến môi trường để Python không tạo file .pyc và log output ngay lập tức
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Lệnh chạy script (thay 'main.py' bằng tên file của bạn)
CMD ["python", "your_script_name.py"]