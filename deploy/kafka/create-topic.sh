#!/bin/bash

# load env
source .env

echo "===== Kafka Config ====="
echo "MAIN_TOPIC=$KAFKA_TOPIC_MAIN"
echo "DLQ_TOPIC=$KAFKA_TOPIC_DLQ"
echo "PARTITIONS=$PARTITIONS"
echo "REPLICATION_FACTOR=$REPLICATION_FACTOR"
echo "BROKER_PORT=$BROKER_PORT"
echo "========================"

create_topic () {
  local TOPIC_NAME=$1

  echo "🚀 Đang tạo topic: $TOPIC_NAME"

  docker exec kafka kafka-topics \
    --create \
    --topic "$TOPIC_NAME" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR" \
    --bootstrap-server "localhost:$BROKER_PORT" \
    --if-not-exists

  if [ $? -eq 0 ]; then
    echo "✅ Topic '$TOPIC_NAME' đã sẵn sàng."
  else
    echo "❌ Lỗi khi tạo topic '$TOPIC_NAME'"
    exit 1
  fi

  echo "📊 Chi tiết topic '$TOPIC_NAME':"

  docker exec kafka kafka-topics \
    --describe \
    --topic "$TOPIC_NAME" \
    --bootstrap-server "localhost:$BROKER_PORT"

  echo "-----------------------------------"
}

# Tạo 2 topic
create_topic "$KAFKA_TOPIC_MAIN"
create_topic "$KAFKA_TOPIC_DLQ"

echo "🎯 Hoàn tất tạo topics!"