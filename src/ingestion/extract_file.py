import requests
import pandas as pd
import os
import time
from src.utils.logger import setup_logger
from dotenv import load_dotenv
import io
from datetime import datetime
import calendar
# load env
load_dotenv() 


logger = setup_logger(
    logger_name="Crawler",
    sub_dir="/app/logging",
    log_file="crawler.log",
    level=10 # logging.DEBUG
)

logger.info("🚀 Đã khởi tạo Logger thành công cho module Extract!")



import boto3
from botocore.exceptions import ClientError
from botocore.client import Config

# Khởi tạo S3 Client kết nối tới MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=os.getenv('MINIO_ENDPOINT'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    config=Config(signature_version='s3v4') # Chuẩn bảo mật cho S3/MinIO
)

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests


def get_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[ 429, 500, 502, 503, 504 ])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

session = get_session()


def get_data_json(session, symbol, start_date_str, end_date_str, page=1, retries=5, delay=1):
    logger.info(f"💪 Đang lấy {symbol}, trang {page} (Từ {start_date_str} đến {end_date_str})")
    url = "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx"
    params = {
        "Symbol": symbol,
        "PageIndex": page,
        "StartDate": start_date_str,
        "EndDate": end_date_str,
        "PageSize": 20
    }

    for attempt in range(retries):
        try:
            res = session.get(url, params=params, timeout=20)
            res.raise_for_status() 

            data = res.json()
            if not data.get("Success", False):
                logger.warning(f"⚠️ Dữ liệu không thành công với {symbol}, trang {page}")
                return pd.DataFrame()

            rows = data.get("Data", {}).get("Data", [])
            
            # Kiểm tra dữ liệu thô trước
            if not rows:
                logger.info(f"Hết dữ liệu tại {symbol}, trang {page} cho giai đoạn này.")
                return pd.DataFrame()

            # KHỞI TẠO DF TRƯỚC (Quan trọng nhất)
            df = pd.DataFrame(rows)

            # Sau đó mới xử lý logic cột
            if 'Symbol' in df.columns:
                df = df.rename(columns={'Symbol': 'code'})
            else:
                df["code"] = symbol 

            time.sleep(delay)
            return df

        except Exception as e:
            logger.error(f"🙏 Lỗi lần thử {attempt + 1}/{retries} tại {symbol}: {e}")
            if attempt == retries - 1:
                logger.critical(f"❌ Thất bại hoàn toàn sau {retries} lần thử cho trang {page}!")
            time.sleep(delay + 1)
            
    return pd.DataFrame()

def generate_monthly_ranges(start_year=2018, start_month=1):

    ranges = []
    now = datetime.now()
    current_year = start_year       # bắt đầu từ 2018
    current_month = start_month     # bắt đầu từ tháng 1

    # Lặp tới khi tới hiện tại 
    while (current_year < now.year) or (current_year == now.year and current_month <= now.month):
        start_date = datetime(current_year, current_month, 1)
        
        last_day = calendar.monthrange(current_year, current_month)[1] # sẽ trả về (weekday, số ngày trong tháng)
        end_date = datetime(current_year, current_month, last_day)

        if end_date > now:
            end_date = now

        ranges.append((
            start_date.strftime("%d/%m/%Y"), 
            end_date.strftime("%d/%m/%Y")
        )) # Ví dụ: ("01/01/2018", "31/01/2018")

        if current_month == 12:
            current_month = 1
            current_year += 1
        else:
            current_month += 1

    return ranges


def get_all_data(symbol):
    logger.info(f"--- Bắt đầu quy trình lấy dữ liệu cho {symbol} ---")

    # 1. Đọc dữ liệu cũ từ MinIO để xác định mốc thời gian bắt đầu crawl
    try:
        old_df = read_df_from_minio(symbol)
        logger.info(f"Đã load file cũ với {len(old_df)} dòng.")
    except Exception:
        logger.info("Chưa có file cũ, sẽ tạo file mới.")
        old_df = pd.DataFrame()

    if not old_df.empty:
        # Chuyển về datetime để lấy max date
        old_df["Ngay"] = pd.to_datetime(old_df["Ngay"], format="%d/%m/%Y", errors="coerce")
        latest_date = old_df["Ngay"].max()
        start_year = latest_date.year
        start_month = latest_date.month
        logger.info(f"Mốc dữ liệu mới nhất: {latest_date.strftime('%d/%m/%Y')}. Bắt đầu crawl từ tháng {start_month}/{start_year}.")
    else:
        latest_date = None
        start_year = 2018
        start_month = 1
        logger.info(f"Không có dữ liệu cũ. Bắt đầu crawl toàn bộ từ {start_year}.")

    # 2. Sinh các khoảng thời gian cần lấy
    month_ranges = generate_monthly_ranges(start_year, start_month)
    all_pages = []

    # 3. Tiến hành crawl từng tháng
    for start_date_str, end_date_str in month_ranges:
        page = 1
        logger.info(f"⏳ Đang lấy dữ liệu {symbol} giai đoạn {start_date_str} - {end_date_str}")
        
        while True:
            df_page = get_data_json(session, symbol, start_date_str, end_date_str, page)
            
            if df_page.empty:
                break # Hết dữ liệu của tháng này, nhảy sang tháng kế tiếp

            df_page["Ngay"] = pd.to_datetime(df_page["Ngay"], format="%d/%m/%Y", errors="coerce")

            # 4. Lọc bỏ các dòng dữ liệu cũ đã có trong MinIO
            if latest_date is not None:
                # Chỉ lấy những record mới hơn latest_date
                df_page = df_page[df_page["Ngay"] > latest_date]
                if df_page.empty:
                    # Nếu sau khi lọc mà df rỗng -> trang này toàn dữ liệu cũ, tiếp tục lặp để check page sau hoặc tháng sau
                    page += 1
                    continue

            all_pages.append(df_page)
            page += 1

    # 5. Xử lý và nối dữ liệu
    if all_pages:
        new_df = pd.concat(all_pages, ignore_index=True) 
        full_df = pd.concat([old_df, new_df], ignore_index=True)
        
        after_count = len(full_df)

        # Chuyển cột Ngay về lại string format dd/MM/yyyy trước khi lưu nếu em muốn đồng nhất chuẩn
        full_df["Ngay"] = full_df["Ngay"].dt.strftime("%d/%m/%Y")

        upload_df_to_minio(full_df, symbol=symbol)

        logger.info(f"👽 Đã lưu {after_count} dòng ")
        return full_df, new_df 
    else:
        logger.info(f"⚡ Dữ liệu của {symbol} đã up-to-date, không có dòng mới nào cần lấy thêm.")
        return old_df, pd.DataFrame()


def read_df_from_minio(symbol):

    bucket_name = os.getenv('MINIO_BUCKET', 'stock-data-lake')
    s3_key = f"raw/stocks/{symbol}/{symbol}.csv"

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        
        if status == 200:
            return pd.read_csv(response['Body'])
        else:
            return pd.DataFrame()
            
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.info(f"Chưa có file cũ trên MinIO cho {symbol}, sẽ tạo mới.")
        else:
            logger.error(f"Lỗi khi đọc file từ MinIO: {e}")
        return pd.DataFrame()
    

def upload_df_to_minio(df, symbol):
    bucket_name = os.getenv('MINIO_BUCKET', 'stock-data-lake')
    s3_object_name = f"raw/stocks/{symbol}/{symbol}.csv" 

    try:
        # Kiểm tra bucket
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except ClientError:
            logger.info(f"🪣 Bucket '{bucket_name}' chưa có, đang tạo mới...")
            s3_client.create_bucket(Bucket=bucket_name)

        # Chuyển DataFrame thành dạng bytes (lưu tạm trên RAM)
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False)
        
        # Đẩy thẳng luồng bytes lên MinIO bằng put_object
        s3_client.put_object(
            Bucket=bucket_name, 
            Key=s3_object_name, 
            Body=csv_buffer.getvalue()
        )
        logger.info(f"☁️ Đã đẩy trực tiếp {symbol} từ RAM lên MinIO tại: {bucket_name}/{s3_object_name}")
        
    except ClientError as e:
        logger.error(f"❌ Lỗi khi tương tác với MinIO ở mã {symbol}: {e}")


# Chỉ khi xài dags
from concurrent.futures import ThreadPoolExecutor, as_completed

def extract_data():
    start_time = time.time()

    symbols = os.getenv("STOCK_SYMBOLS").split(",")

    max_threads = int(os.getenv("MAX_THREADS", 4)) 
    logger.info(f"⚡ Chạy multi-thread với {max_threads} luồng")

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {executor.submit(get_all_data, symbol): symbol for symbol in symbols}
        # Thu hoạch kết quả (công việc nào xong trước thì trả về kết quả luôn)
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                future.result() 
                logger.info(f"✅ Hoàn thành crawl {symbol}")
            except Exception as e:
                logger.exception(f"❌ Lỗi khi crawl {symbol}: {e}")

    duration = time.time() - start_time
    logger.info(f"😵‍💫 Hoàn tất toàn bộ quy trình trong {duration:.2f} giây!")

# if __name__ == "__main__":
#     extract_data()