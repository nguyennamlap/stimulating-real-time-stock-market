import pandera.pandas as pa
from schemas import stock_schema, validate_stock_data
from src.utils.logger import setup_logger
import pandas as pd
from pandera.errors import SchemaError, SchemaErrors


logger = setup_logger(
    logger_name="Validator",
    sub_dir="/app/logging",
    log_file="validation.log",
    level=10
)

logger.info("🚀 Đã khởi tạo Logger thành công cho module Valiadte !")


def process_all_stocks(df,stock_list):
    """Process validation for all stocks with robust error handling"""
    results = []
    
    for stock_code in stock_list:

        result = {
            'stock': stock_code,
            'status': 'UNKNOWN', # Default status
            'rows': 0,
            'error': None
        }
        
        try:
            # 2. Kiểm tra Schema cơ bản (tránh KeyError sâu bên trong)
            required_columns = ['Ngay']
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                raise KeyError(f"Missing columns: {missing_cols}")

            validated_df = validate_stock_data(df, stock_code)
            
            # 4. Success
            result.update({
                'status': 'SUCCESS',
                'rows': len(validated_df),
                'date_range': {
                    'min': validated_df['Ngay'].min(),
                    'max': validated_df['Ngay'].max()
                }
            })
            logger.info(f"✓ {stock_code}: Validation successful")
            
        
        except FileNotFoundError:
            result['status'] = 'FILE_NOT_FOUND'
            result['error'] = 'File does not exist'
            logger.error(f"✗ {stock_code}: File not found")

        except pd.errors.EmptyDataError:
            result['status'] = 'EMPTY_FILE'
            result['error'] = 'CSV file is empty'
            logger.error(f"✗ {stock_code}: File is empty")
            
        except pd.errors.ParserError:
            result['status'] = 'PARSING_ERROR'
            result['error'] = 'Malformed CSV structure'
            logger.error(f"✗ {stock_code}: Cannot parse CSV structure")

        except KeyError as e:
            result['status'] = 'SCHEMA_MISMATCH'
            result['error'] = str(e)
            logger.error(f"✗ {stock_code}: Missing required columns")
        except SchemaErrors as err:
            missing_cols = err.failure_cases[err.failure_cases['check'] == 'column_in_dataframe']['column'].unique()
            
            result['status'] = 'SCHEMA_MISMATCH'
            result['error'] = f"Missing columns: {list(missing_cols)}"
            logger.error(f"✗ {stock_code}: Missing columns: {list(missing_cols)}")
        except PermissionError:
            result['status'] = 'PERMISSION_DENIED'
            result['error'] = 'Access denied to file'
            logger.error(f"✗ {stock_code}: Permission denied")


        except Exception as e:
            # Catch-all cho các lỗi không ngờ tới (MemoryError, ZeroDivision, v.v.)
            result['status'] = 'INTERNAL_ERROR'
            result['error'] = f"{type(e).__name__}: {str(e)}"
            logger.critical(f"☠ {stock_code}: Unexpected crash - {e}", exc_info=True)
        
        results.append(result)

    return result
