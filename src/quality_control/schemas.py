import pandera.pandas as pa
from pandera import Column, DataFrameSchema, Check
import pandas as pd
from datetime import datetime, date
from typing import Union
from src.utils.logger import setup_logger
from dotenv import load_dotenv
import pandera.errors as pa_errors
load_dotenv()
# Setup logging
import pandas as pd
import boto3
from io import StringIO

logger = setup_logger(
    logger_name="Validator",
    sub_dir="/app/logging",
    log_file="validation.log",
    level=10
)

logger.info("🚀 Đã khởi tạo Logger thành công cho module Valiadte !")


class ValidationLogger:
    """Logger cho validation errors"""
    _errors = []
    # cls = đại diện cho chính cái class đó
    @classmethod # Không cần ValidationLogger()
    def add_error(cls, error_type: str, value: str, row_index=None):
        """Thêm error vào buffer"""
        error = {
            'type': error_type,
            'value': str(value),
            'row_index': row_index, # row index là lỗi ở đâu 
            'timestamp': datetime.now()
        }
        cls._errors.append(error) # ➡️ Ném lỗi vào “thùng lỗi chung
    
    @classmethod
    def get_errors(cls):
        """Lấy tất cả errors"""
        return cls._errors 
    
    @classmethod
    def clear_errors(cls):
        """Xóa tất cả errors"""
        cls._errors = []
    
    @classmethod
    def log_summary(cls, stock_code: str, total_rows: int):
        """Log summary sau khi validation"""
        if cls._errors:
            error_count = len(cls._errors)
            logger.error(f"[{stock_code}] Validation failed: {error_count}/{total_rows} rows have errors")
            
            # Group errors by type
            from collections import Counter
            error_types = Counter([e['type'] for e in cls._errors])
            # Lấy tất cả error['type']
            # Đếm xem mỗi loại lỗi xuất hiện bao nhiêu lần

            for error_type, count in error_types.items():
                logger.error(f"  - {error_type}: {count} errors")
            
            # Log first 5 errors as sample
            for i, error in enumerate(cls._errors[:5], 1):
                logger.error(f"  Sample error {i}: {error['type']} - Value: {error['value']}")
            
            if error_count > 5:
                logger.error(f"  ... and {error_count - 5} more errors")
        else:
            logger.info(f"[{stock_code}] Validation passed: {total_rows} rows")


def parse_date_safe(date_val: Union[str, datetime, date]):
    """Parse date an toàn, không throw exception"""
    if pd.isna(date_val): # Check giá trị rỗng (rất chuẩn)
        return None
    
    # Nếu đã là datetime/date object
    if isinstance(date_val, (datetime, date)):
        return date_val if isinstance(date_val, date) else date_val.date()
    
    # Nếu là string
    if isinstance(date_val, str):
        date_str = date_val.strip()
        for fmt in ['%Y-%m-%d', '%d/%m/%Y']:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
    return None # Nếu xử lý ko được thì trả None cho ló sạch


def validate_date_format(series: pd.Series) -> pd.Series:
    """Validate date format (YYYY-MM-DD or DD/MM/YYYY)"""
    results = []
    
    for idx, date_val in series.items():
        parsed = parse_date_safe(date_val)
        if parsed is None:
            ValidationLogger.add_error(
                "INVALID_DATE_FORMAT", 
                date_val, 
                row_index=idx
            ) 
            results.append(False)
        else:
            results.append(True)
    
    return pd.Series(results, index=series.index) 
    # “Kết quả validate này thuộc về đúng dòng nào thì gắn lại đúng dòng đó.”


def is_not_weekend(series: pd.Series) -> pd.Series:
    """Check if date is not a weekend"""
    results = []
    
    for idx, date_val in series.items():
        parsed = parse_date_safe(date_val)
        if parsed is None:
            results.append(False)
        else:
            is_weekday = parsed.weekday() < 5
            if not is_weekday:
                ValidationLogger.add_error(
                    "DATE_IS_WEEKEND",
                    f"{date_val} ({parsed.strftime('%A')})",
                    row_index=idx
                )
            results.append(is_weekday)
    
    return pd.Series(results, index=series.index)


def is_not_future(series: pd.Series) -> pd.Series:
    """Check if date is not in the future"""
    results = []
    today = date.today()
    
    for idx, date_val in series.items():
        parsed = parse_date_safe(date_val)
        if parsed is None:
            results.append(False)
        else:
            is_not_future = parsed <= today
            if not is_not_future:
                ValidationLogger.add_error(
                    "DATE_IS_FUTURE",
                    f"{date_val} (today: {today})",
                    row_index=idx
                )
            results.append(is_not_future)
    
    return pd.Series(results, index=series.index)


def volume_value_consistency(df: pd.DataFrame) -> pd.Series:
    """Khối lượng khớp lệnh = 0 thì Giá trị khớp lệnh phải = 0"""
    return ~((df['KhoiLuongKhopLenh'] == 0) & (df['GiaTriKhopLenh'] != 0))

def negotiated_volume_value_consistency(df: pd.DataFrame) -> pd.Series:
    """Khối lượng thỏa thuận = 0 thì Giá trị thỏa thuận phải = 0"""
    return ~((df['KLThoaThuan'] == 0) & (df['GtThoaThuan'] != 0))

def price_range_open(df: pd.DataFrame) -> pd.Series:
    """Giá Thấp Nhất <= Giá Mở Cửa <= Giá Cao Nhất"""
    return (df['GiaThapNhat'] <= df['GiaMoCua']) & (df['GiaMoCua'] <= df['GiaCaoNhat'])

def price_range_close(df: pd.DataFrame) -> pd.Series:
    """Giá Thấp Nhất <= Giá Đóng Cửa <= Giá Cao Nhất"""
    return (df['GiaThapNhat'] <= df['GiaDongCua']) & (df['GiaDongCua'] <= df['GiaCaoNhat'])

def high_low_consistency(df: pd.DataFrame) -> pd.Series:
    """Giá Cao Nhất >= Giá Thấp Nhất"""
    return df['GiaCaoNhat'] >= df['GiaThapNhat']

# Đây là bản hiến pháp dữ liệu 📜
stock_schema = DataFrameSchema(
    columns={
        "Ngay": Column(
            "datetime64[ns]", # Để Pandera check kiểu datetime xịn luôn
            checks=[
                Check(lambda s: s.dt.dayofweek < 5, name="is_not_weekend"),
                Check(lambda s: s <= pd.Timestamp.now(), name="is_not_future")
            ],
            nullable=False
        ),
        "GiaDieuChinh": Column(
            float,
            checks=[
                Check.greater_than_or_equal_to(0, error="GiaDieuChinh must be >= 0")
            ],
            nullable=False,
            description="Reference/Adjusted price"
        ),
        "GiaDongCua": Column(
            float,
            checks=[
                Check.greater_than_or_equal_to(0, error="GiaDongCua must be >= 0")
            ],
            nullable=False,
            description="Closing price"
        ),
        "ThayDoi": Column(
            float,
            nullable=False,
            description="Price change (can be negative, positive, or zero)"
        ),
        "KhoiLuongKhopLenh": Column(
            float,
            checks=[
                Check.greater_than_or_equal_to(0, error="KhoiLuongKhopLenh must be >= 0")
            ],
            nullable=False,
            description="Matched volume"
        ),
        "GiaTriKhopLenh": Column(
            float,
            checks=[
                Check.greater_than_or_equal_to(0, error="GiaTriKhopLenh must be >= 0")
            ],
            nullable=False,
            description="Matched transaction value"
        ),
        "KLThoaThuan": Column(
            float,
            checks=[
                Check.greater_than_or_equal_to(0, error="KLThoaThuan must be >= 0")
            ],
            nullable=False,
            description="Negotiated volume"
        ),
        "GtThoaThuan": Column(
            float,
            checks=[
                Check.greater_than_or_equal_to(0, error="GtThoaThuan must be >= 0")
            ],
            nullable=False,
            description="Negotiated transaction value"
        ),
        "GiaMoCua": Column(
            float,
            checks=[
                Check.greater_than_or_equal_to(0, error="GiaMoCua must be >= 0")
            ],
            nullable=False,
            description="Opening price"
        ),
        "GiaCaoNhat": Column(
            float,
            checks=[
                Check.greater_than_or_equal_to(0, error="GiaCaoNhat must be >= 0")
            ],
            nullable=False,
            description="Highest price"
        ),
        "GiaThapNhat": Column(
            float,
            checks=[
                Check.greater_than_or_equal_to(0, error="GiaThapNhat must be >= 0")
            ],
            nullable=False,
            description="Lowest price"
        ),
    },
    checks=[
        Check(volume_value_consistency, error="GiaTriKhopLenh must be 0 when KhoiLuongKhopLenh is 0"),
        Check(negotiated_volume_value_consistency, error="GtThoaThuan must be 0 when KLThoaThuan is 0"),
        Check(price_range_open, error="GiaMoCua must be within [GiaThapNhat, GiaCaoNhat]"),
        Check(price_range_close, error="GiaDongCua must be within [GiaThapNhat, GiaCaoNhat]"),
        Check(high_low_consistency, error="GiaCaoNhat must be >= GiaThapNhat"),
    ],
    strict=True,
    coerce=True
)


def validate_stock_data(df: pd.DataFrame, stock_code: str = "UNKNOWN"):
    ValidationLogger.clear_errors()
    
    # Tiền xử lý: Xóa cột 'code' nếu nó làm Schema báo lỗi strict
    temp_df = df.copy()
    if 'code' in temp_df.columns:
        temp_df = temp_df.drop(columns=['code'])

    try:
        # Validate toàn bộ DataFrame
        validated_df = stock_schema.validate(temp_df, lazy=True)
        return validated_df, pd.DataFrame() # Trả về df rỗng cho phần lỗi

    except pa_errors.SchemaErrors as err:
        # 1. Lấy danh sách các index bị lỗi (Unique)
        error_indices = (
            err.failure_cases['index']
            .dropna()
            .astype(int)
            .unique()
        )
        
        # 2. Phân tách DataFrame gốc dựa trên index lỗi
        bad_data_df = df.loc[df.index.isin(error_indices)]
        
        # Những dòng KHÔNG NẰM TRONG danh sách index lỗi -> Good Data
        good_data_df = df.loc[~df.index.isin(error_indices)]
        
        # 3. Logging tóm tắt
        logger.warning(f"⚠️ [{stock_code}] Phân luồng xong: {len(good_data_df)} dòng sạch, {len(bad_data_df)} dòng lỗi.")
        
        # Lưu failure_cases (chi tiết lỗi) để phục vụ debug sau này
        error_log_path = f"/app/logging/schema_errors_{stock_code}.csv"
        err.failure_cases.to_csv(error_log_path, index=False)

        # Trả về cả hai để phía sau xử lý đẩy vào 2 Topic Kafka khác nhau
        return good_data_df, bad_data_df  
    
# def main():
#     symbol = "VCB"

#     s3 = boto3.client(
#         "s3",
#         endpoint_url="http://minio:9000",
#         aws_access_key_id="minioadmin",
#         aws_secret_access_key="minioadmin",
#     )

#     bucket = "stock-data"
#     key = f"processed/stocks/{symbol}/{symbol}.csv"

#     # 1. Load data đã clean
#     obj = s3.get_object(Bucket=bucket, Key=key)
#     df = pd.read_csv(obj["Body"])

#     # 2. Validate
#     good_df, bad_df = validate_stock_data(df, stock_code=symbol)

#     # 3. Quyết định PASS / FAIL
#     if len(bad_df) == 0:
#         print("PASS")
#     else:
#         print("FAIL")
#         print(f"Bad rows: {len(bad_df)}")

# if __name__ == "__main__":
#     main()