#!/bin/bash

# 1. Khai báo tên network (Bạn có thể đổi tên tùy ý)
NETWORK_NAME="your-network"

echo "------------------------------------------"
echo "Đang kiểm tra Docker Network: $NETWORK_NAME"

# 2. Kiểm tra xem network đã tồn tại chưa
if [ "$(docker network ls | grep $NETWORK_NAME)" ]; then
  echo "=> Network '$NETWORK_NAME' đã tồn tại. Không cần tạo mới."
else
  echo "=> Đang tạo Network '$NETWORK_NAME'..."
  
# 3. Lệnh tạo network (driver bridge là mặc định cho môi trường đơn máy)
  docker network create --driver bridge $NETWORK_NAME
  
  if [ $? -eq 0 ]; then
    echo "=> Thành công: Đã tạo network '$NETWORK_NAME'."
  else
    echo "=> Thất bại: Có lỗi xảy ra khi tạo network."
    exit 1
  fi
fi

echo "------------------------------------------"
echo "Danh sách các network hiện có:"
docker network ls