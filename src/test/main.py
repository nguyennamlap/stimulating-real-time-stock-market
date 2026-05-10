import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
# Import các hàm của bạn (thay đổi đường dẫn cho phù hợp)
from src.ingestion.extract_file import get_all_data
from src.quality_control.schemas import validate_stock_data
from src.streaming.python_producer.kafka_producer import send_to_kafka
from src.streaming.python_producer.check_data import get_load_mode
from src.utils.logger import setup_logger
import re
import pandas as pd


logger = setup_logger("InRamPipeline", "/app/logging", "pipeline.log", 10)

# 🔥======================================== CLEAN DATA ========================================🔥

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
        
    df = df.copy()

    # 1. Xử lý cột 'Ngay'
    df['Ngay'] = pd.to_datetime(df['Ngay'], dayfirst=True, errors='coerce')

    # 2. Xử lý cột 'ThayDoi': "4(3,64 %)" -> 4.0
    if 'ThayDoi' in df.columns and df['ThayDoi'].dtype == object:
        df['ThayDoi'] = (
            df['ThayDoi'].astype(str)
            .str.replace(',', '.')
            .str.extract(r'([-]?\d*\.?\d+)')
            .astype(float)
        )
    # 3. Ép kiểu hàng loạt cho các cột số (Xử lý định dạng VN: 1.000,5 -> 1000.5)
    numeric_cols = [
        'GiaDieuChinh', 'GiaDongCua', 'GiaMoCua', 
        'GiaCaoNhat', 'GiaThapNhat', 'GiaTriKhopLenh', 'GtThoaThuan',
        'KhoiLuongKhopLenh', 'KLThoaThuan' 
    ]
    for col in numeric_cols:
        if col in df.columns:
            if df[col].dtype == object:
                # Xóa dấu chấm phân cách hàng nghìn, đổi phẩy thành chấm thập phân
                df[col] = (
                        df[col].astype(str)
                        .str.replace('.', '', regex=False)
                        .str.replace(',', '.', regex=False)
                        )
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


# 🔥======================================== PROCESS ========================================🔥

def process_single_stock(symbol: str):
    # Check data để chọn chế độ 
    load_mode = get_load_mode(os.getenv('KAFKA_TOPIC_MAIN'))

    try:
        logger.info(f"[{symbol}] BƯỚC 1: Lấy dữ liệu với chế độ {load_mode.upper()}...")
        raw_df, new_df = get_all_data(symbol) 
        
        # Chọn Dataframe dựa trên chế độ chạy
        if load_mode == 'full':
            target_df = raw_df
            logger.info("Chế độ full")
        elif load_mode == 'incremental':
            target_df = new_df
            logger.info("Chế độ incremental")
        else:
            logger.error(f"[{symbol}] load_mode không hợp lệ: {load_mode}. Vui lòng chọn 'full' hoặc 'incremental'.")
            return

        # Kiểm tra xem có dữ liệu để xử lý không
        if target_df is None or target_df.empty:
            logger.info(f"[{symbol}] Không có dữ liệu ở chế độ {load_mode.upper()}. Dừng luồng.")
            return

        # Bước 2: Validate Data (Sử dụng target_df đã chọn)
        logger.info(f"[{symbol}] BƯỚC 2: Làm sạch và kiểm tra dữ liệu...")
        cleaned_df = clean_data(target_df)
        good_df, bad_df = validate_stock_data(cleaned_df, stock_code=symbol)

        # Bước 3: Đẩy vào Kafka 
        logger.info(f"[{symbol}] BƯỚC 3: Phân luồng dữ liệu vào Kafka...")
        
        main_topic = os.getenv("KAFKA_TOPIC_MAIN", "clean_stock_data")
        dlq_topic = os.getenv("KAFKA_TOPIC_DLQ", "dead_letter_queue_stock")

        # 3.1 Đẩy dữ liệu SẠCH vào Topic chính
        if good_df is not None and not good_df.empty:
            good_df['Ngay'] = good_df['Ngay'].dt.strftime('%d/%m/%Y') 
            send_to_kafka(good_df, topic_name=main_topic)
            logger.info(f"✅ [{symbol}] Đã đẩy {len(good_df)} dòng SẠCH vào topic '{main_topic}'")
        else:
            logger.warning(f"⚠️ [{symbol}] Không có dòng dữ liệu sạch nào để đẩy!")

        # 3.2 Đẩy dữ liệu LỖI vào Topic DLQ
        if bad_df is not None and not bad_df.empty:
            bad_df['Ngay'] = bad_df['Ngay'].dt.strftime('%d/%m/%Y')
            send_to_kafka(bad_df, topic_name=dlq_topic)
            logger.warning(f"🚨 [{symbol}] Đã đẩy {len(bad_df)} dòng LỖI vào DLQ topic '{dlq_topic}'.")
        
        logger.info(f"🎉 [{symbol}] Hoàn tất Pipeline thành công!")

    except Exception as e:
        logger.error(f"❌ [{symbol}] Pipeline thất bại: {e}", exc_info=True)


# 🔥======================================== RUN PIPELINE ========================================🔥

def run_in_ram_pipeline():
    symbols = os.getenv("STOCK_SYMBOLS", "FPT,VNM,VIC").split(",")
    max_threads = int(os.getenv("MAX_THREADS", 2))
    
    start_time = time.time()
    logger.info(f"🚀 Khởi động In-RAM Pipeline với {max_threads} luồng cho {len(symbols)} mã...")

    # Chạy Multi-threading cho toàn bộ pipeline
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {executor.submit(process_single_stock, symbol): symbol for symbol in symbols}
        
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                future.result(timeout=60)
            except Exception as e:
                logger.error(f"Lỗi Thread ở mã {symbol}: {e}")

    logger.info(f"🏁 Tổng thời gian chạy E2E: {time.time() - start_time:.2f} giây")


# 🔥======================================== CALL FUNCTION ========================================🔥

if __name__ == "__main__":
    run_in_ram_pipeline()

    # Dành cho hệ thống real-time thật
    # while True:
    #     run_in_ram_pipeline()
    #     time.sleep(30) 