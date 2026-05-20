# Stimulating Real-Time Stock Market

Hệ thống xử lý dữ liệu chứng khoán theo hướng **batch + streaming**, kết hợp **Airflow, Kafka, Spark, DBT, Postgres** và các lớp kiểm tra chất lượng dữ liệu để tạo pipeline gần real-time.

## Mục tiêu dự án

* Crawl dữ liệu chứng khoán từ nguồn đầu vào.
* Kiểm tra và làm sạch dữ liệu trước khi nạp vào hệ thống.
* Đẩy dữ liệu vào Kafka để xử lý streaming.
* Tính các chỉ báo kỹ thuật trong Spark.
* Biến đổi dữ liệu bằng DBT để tạo các bảng mart phục vụ phân tích.
* Theo dõi trạng thái pipeline và log theo từng lớp.

## Kiến trúc tổng quan

Luồng xử lý chính:

1. **Ingestion**: crawl / trích xuất dữ liệu.
2. **Quality Control**: validate schema, kiểm tra dữ liệu bẩn.
3. **Streaming Producer**: đẩy dữ liệu vào Kafka.
4. **Spark Processor**: xử lý stream, tính technical indicators.
5. **Orchestration**: Airflow điều phối các job.
6. **Transformation**: DBT xây dựng staging / intermediate / marts.
7. **Monitoring & Logging**: theo dõi trạng thái và ghi log.

## Tính năng chính

* Crawl dữ liệu chứng khoán.
* Validate dữ liệu bằng schema.
* Stream dữ liệu qua Kafka.
* Xử lý dữ liệu thời gian thực bằng Spark.
* Tính các chỉ báo kỹ thuật trong pipeline.
* Mô hình hóa dữ liệu bằng DBT:

  * **staging**
  * **intermediate**
  * **marts**
* Có test dữ liệu cơ bản như:

  * `not_null_ticker`
  * `positive_price`
  * `unique_ticker_date`
* Có logging riêng cho từng thành phần.

## Cấu trúc thư mục

```text
.
├── airflow/                 # DAG Airflow
├── dbt/                     # DBT project, profiles, seeds, snapshots, tests
├── deploy/                  # Docker Compose cho Kafka / Airflow / Spark / local debug
├── logging/                 # Log files
├── scripts/                 # Script tạo bảng, tạo key, network...
└── src/                     # Source code chính
    ├── ingestion/          # Trích xuất / crawl dữ liệu
    ├── quality_control/    # Kiểm tra chất lượng dữ liệu
    ├── streaming/          # Kafka producer, Spark processor, monitoring
    ├── test/               # Test container / test runner
    └── utils/              # Logger, tiện ích chung
```

## Yêu cầu hệ thống

* Docker
* Docker Compose
* Python 3.10+ (tuỳ môi trường)
* Apache Airflow
* Kafka
* Spark
* Postgres
* DBT

## Cài đặt nhanh

### 1. Clone repository

```bash
git clone <repo-url>
cd stimulating-real-time-stock-market
```

### 2. Tạo network / chuẩn bị môi trường

Nếu dự án của bạn dùng network riêng cho Docker:

```bash
bash scripts/networks.sh
```

### 3. Tạo bảng trong database

```bash
psql -h <host> -U <user> -d <db> -f scripts/create_table.sql
```

### 4. Tạo Fernet key cho Airflow

```bash
python scripts/generate_fernet_key.py
```

## Chạy các dịch vụ bằng Docker Compose

### Kafka

```bash
cd deploy/kafka
docker compose -f docker-compose.kafka.yml up -d
```

### Airflow

```bash
cd deploy/orchestration
docker compose -f docker-compose.airflow.yml up -d --build
```

### Spark

```bash
cd deploy/spark
docker compose -f docker-compose.spark.yml up -d --build
```

### Local debug crawl

```bash
cd deploy/local_to_debug
docker compose -f docker-compose.crawl.yml up -d --build
```

## DBT

DBT project nằm trong:

```text
dbt/stock_dbt_project
```

### Các layer DBT

* **staging**: chuẩn hóa dữ liệu nguồn.
* **intermediate**: tạo logic trung gian như gap, rank, trend, feature engineering.
* **marts**: tạo bảng phục vụ phân tích như dim/fact/signal.

### Chạy DBT

Ví dụ:

```bash
cd dbt/stock_dbt_project
dbt debug
dbt deps
dbt seed
dbt run
dbt test
```

## Airflow DAG

DAG chính nằm ở:

```text
airflow/dags/stock_data_dag.py
```

DAG này có thể dùng để:

* crawl dữ liệu
* validate dữ liệu
* đẩy dữ liệu vào pipeline streaming
* kích hoạt job xử lý tiếp theo

## Ingestion

Thư mục:

```text
src/ingestion
```

Chứa logic trích xuất / crawl dữ liệu và file requirements riêng cho container ingestion.

## Quality Control

Thư mục:

```text
src/quality_control
```

Chức năng:

* định nghĩa schema kiểm tra dữ liệu
* validate dữ liệu đầu vào
* loại bỏ hoặc đánh dấu bản ghi lỗi

## Streaming

Thư mục:

```text
src/streaming
```

Gồm 3 phần chính:

* **monitoring**: health check, kiểm tra driver/stream.
* **python_producer**: producer đẩy dữ liệu vào Kafka.
* **spark-processor**: xử lý stream bằng Spark.

### Producer

```text
src/streaming/python_producer/kafka_producer.py
```

### Spark processor

```text
src/streaming/spark-processor/spark_stream_processor.py
```

### Technical indicators

```text
src/streaming/spark-processor/logic/technical_indicators.py
```

## Logging

Log được gom theo từng nhóm:

* `logging/kafka_log/`
* `logging/spark_logs/`

Mỗi service có thể ghi log riêng để dễ debug khi pipeline lỗi.

## Tests

Thư mục:

```text
src/test
```

Ngoài ra DBT cũng có test riêng trong:

```text
dbt/stock_dbt_project/tests
```

### DBT tests đang có

* `not_null_ticker.sql`
* `positive_price.sql`
* `unique_ticker_date.sql`

## Các model DBT tiêu biểu

### Staging

* `stg_stock.sql`
* `stg_stock_clean.sql`

### Intermediate

* `feature_engineering.sql`
* `gap.sql`
* `rank.sql`
* `trend.sql`

### Marts

* `dim_stock.sql`
* `dim_time.sql`
* `fact_stock.sql`
* `stock_signal.sql`

## Seed và snapshot

* `seeds/stock_info.csv`: dữ liệu seed.
* `snapshots/snap_stock.sql`: snapshot lịch sử dữ liệu.

## Troubleshooting

### 1. Kafka không tạo được topic

Kiểm tra script:

```bash
bash deploy/kafka/create-topic.sh
```

### 2. Airflow không đọc được biến / key

Kiểm tra:

* `profiles.yml`
* `Dockerfile.airflow`
* Fernet key
* connection string tới Postgres

### 3. DBT không connect được database

Kiểm tra file:

```text
dbt/profiles/profiles.yml
```

### 4. Spark job lỗi dependency

Kiểm tra file requirements trong:

```text
src/streaming/spark-processor/requirements.txt
```

## Gợi ý mở rộng

* Thêm dashboard quan sát realtime.
* Thêm alert Telegram / email khi pipeline fail.
* Thêm checkpointing cho streaming.
* Thêm backfill theo lịch.
* Thêm metric theo dõi độ trễ, số bản ghi, tỷ lệ lỗi.

## License

Dự án hiện đang dùng file `LICENSE` ở root repository.


