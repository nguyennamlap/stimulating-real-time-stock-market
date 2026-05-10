from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType
import pandas as pd


OUTPUT_SCHEMA = StructType([
    # feature cũ
    StructField("code", StringType(), True),
    StructField("ngay_ts", TimestampType(), True),
    StructField("giadongcua", DoubleType(), True),
    StructField("giadieuchinh", DoubleType(), True),
    StructField("thaydoi", DoubleType(), True),
    StructField("khoiluongkhoplenh", LongType(), True),
    StructField("giatrikhoplenh", DoubleType(), True),
    StructField("giamocua", DoubleType(), True),
    StructField("giacaonhat", DoubleType(), True),
    StructField("giathapnhat", DoubleType(), True),
    # Các cột feature mới
    StructField("EMA_20", DoubleType(), True),
    StructField("EMA_50", DoubleType(), True),
    StructField("EMA_200", DoubleType(), True),
    StructField("RSI", DoubleType(), True),
    StructField("macd_line", DoubleType(), True),
    StructField("macd_signal", DoubleType(), True),
    StructField("macd_hist", DoubleType(), True),
    StructField("bb_middle", DoubleType(), True),
    StructField("bb_upper", DoubleType(), True),
    StructField("bb_lower", DoubleType(), True)
])

def calculate_indicators_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Hàm xử lý nội bộ cho từng nhóm (mã chứng khoán)
    """
    # Sắp xếp thời gian trong nội bộ nhóm
    pdf = pdf.sort_values("ngay_ts")

    # --- EMA ---
    for p in [20, 50, 200]:
        pdf[f"EMA_{p}"] = pdf["giadongcua"].ewm(span=p, adjust=False).mean()

    # --- RSI (14) ---
    delta = pdf["giadongcua"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False).mean()
    rs = avg_gain / avg_loss
    pdf["RSI"] = 100 - (100 / (1 + rs))

    # --- MACD ---
    ema12 = pdf["giadongcua"].ewm(span=12, adjust=False).mean()
    ema26 = pdf["giadongcua"].ewm(span=26, adjust=False).mean()
    pdf["macd_line"] = ema12 - ema26
    pdf["macd_signal"] = pdf["macd_line"].ewm(span=9, adjust=False).mean()
    pdf["macd_hist"] = pdf["macd_line"] - pdf["macd_signal"]

    # --- Bollinger Bands ---
    window = pdf["giadongcua"].rolling(window=20)
    pdf["bb_middle"] = window.mean()
    std = window.std(ddof=0)
    pdf["bb_upper"] = pdf["bb_middle"] + (2 * std)
    pdf["bb_lower"] = pdf["bb_middle"] - (2 * std)

    return pdf

def add_technical_indicators(df: DataFrame) -> DataFrame:
    # Chỉ giữ lại các cột cần thiết để giảm tải trọng truyền tin
    df_clean = df.drop("Ngay", "gtthoathuan", "klthoathuan")

    return df_clean.groupby("code").applyInPandas(
        calculate_indicators_pandas, 
        schema=OUTPUT_SCHEMA
    )
    # Chia nhóm theo 'code' và áp dụng hàm Pandas
