
IMAGE_TAG ?= latest


build:
	docker build -t stock_ingestion_in_ram:$(IMAGE_TAG) -f src/test/Dockerfile .
	docker build -t stock_spark:$(IMAGE_TAG) -f src/streaming/spark-processor/Dockerfile.spark .
	docker build -t stock_dbt:$(IMAGE_TAG) -f dbt/Dockerfile dbt

up-infra:
	docker-compose -f deploy/kafka/docker-compose.kafka.yml up -d
	docker-compose -f deploy/spark/docker-compose.spark.yml up -d

down-infra:
	docker-compose -f deploy/kafka/docker-compose.kafka.yml down
	docker-compose -f deploy/spark/docker-compose.spark.yml down

up-airflow:
	docker-compose -f deploy/airflow/docker-compose.airflow.yml up -d

down-airflow:
	docker-compose -f deploy/airflow/docker-compose.airflow.yml down


dbt-run:
	docker run --rm stock_dbt:$(IMAGE_TAG) dbt run --profiles-dir /root/.dbt

dbt-test:
	docker run --rm stock_dbt:$(IMAGE_TAG) dbt test --profiles-dir /root/.dbt

run:
	make build
	make up-infra
	make up-airflow

down:
	make down-airflow
	make down-infra


# make build         build hết image
# make up-infra      chạy Kafka + Spark
# make up-airflow    chạy Airflow
# make down          tắt hết