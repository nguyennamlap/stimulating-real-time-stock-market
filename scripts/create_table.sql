-- Tạo schema
CREATE SCHEMA IF NOT EXISTS spark_streaming;

-- Tạo bảng raw
CREATE TABLE IF NOT EXISTS spark_streaming.stock_table (

    -- BUSINESS COLUMNS 
    code TEXT NOT NULL,                    
    ngay_ts DATE NOT NULL,                  

    giadieuchinh NUMERIC,
    giadongcua NUMERIC,
    giamocua NUMERIC,
    giacaonhat NUMERIC,
    giathapnhat NUMERIC,

    khoiluongkhoplenh BIGINT,
    giatrikhoplenh NUMERIC,
    klthoathuan BIGINT,
    gtthoathuan NUMERIC,

    thaydoi NUMERIC,

    -- indicators
    ema_20 NUMERIC,
    ema_50 NUMERIC,
    ema_200 NUMERIC,
    rsi NUMERIC,

    macd_line NUMERIC,
    macd_signal NUMERIC,
    macd_hist NUMERIC,

    bb_middle NUMERIC,
    bb_upper NUMERIC,
    bb_lower NUMERIC,

    -- METADATA (RẤT QUAN TRỌNG)

    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,


    -- TECHNICAL

    id BIGSERIAL PRIMARY KEY
);

-- Index
-- CREATE INDEX IF NOT EXISTS idx_raw_stock 
-- ON raw.stock_price (ticker, trading_date);