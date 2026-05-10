import logging
import os
from logging.handlers import RotatingFileHandler

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOG_ROOT_DIR = os.path.join(BASE_DIR, 'logging')

def setup_logger(logger_name, sub_dir, log_file, level=logging.INFO):

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # Tránh việc add handler nhiều lần nếu hàm được gọi lại
    if not logger.handlers:
        # Formatter chung cho toàn project
        formatter = logging.Formatter(
            '%(asctime)s | %(name)-15s | %(levelname)-8s | %(message)s'
        )

        # Handler 1: In ra Console (Rất quan trọng khi chạy bằng Docker Compose)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Handler 2: Ghi ra File
        # Tạo thư mục con nếu chưa tồn tại (vd: logging/crawl_log)
        target_dir = os.path.join(LOG_ROOT_DIR, sub_dir)
        os.makedirs(target_dir, exist_ok=True)
        
        file_path = os.path.join(target_dir, log_file)
        
        # Dùng RotatingFileHandler để file log không phình to quá mức (tối đa 10MB/file, giữ 3 file cũ)
        file_handler = RotatingFileHandler(file_path, maxBytes=10*1024*1024, backupCount=3)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger