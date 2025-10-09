#!/usr/bin/env python3
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import functools
import time

# --- Load .env (nếu có) ---
load_dotenv()
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "123456")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "studentdb")

# --- Tạo engine SQLAlchemy ---
engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# --- Decorator để đo thời gian ---
def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        t0 = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - t0

        # Lấy số batch từ return value (hàm trả về số batches)
        batches = "unknown"
        if isinstance(result, int):
            batches = result
        elif isinstance(result, tuple) and len(result) > 0:
            try:
                batches = int(result[0])
            except Exception:
                batches = "unknown"

        print(f"finished transfering {batches} batches trong {elapsed:.2f} giây")
        return result
    return wrapper

# --- Hàm chuyển dữ liệu theo chunk ---
@timeit
def transfer_csv_to_postgres(csv_path: str,
                             table_name: str,
                             engine,
                             chunksize: int = 5,
                             show_progress: bool = True):
    """
    Đọc csv theo chunk và ghi vào PostgreSQL.
    Trả về số batches đã chuyển.
    """
    batches = 0
    # chunksize trả về iterator (TextFileReader)
    for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunksize)):
        # Với chunk đầu tiên tạo lại bảng (replace) để chắc chắn schema phù hợp.
        if i == 0:
            chunk.to_sql(table_name, engine, index=False, if_exists='replace')
        else:
            chunk.to_sql(table_name, engine, index=False, if_exists='append')

        batches += 1
        if show_progress:
            print(f" -> batch {batches} transferred ({len(chunk)} rows)")

    return batches

# --- Main ---
if __name__ == "__main__":
    CSV_PATH = "dsl.csv"    # sửa path nếu file ở chỗ khác
    TABLE_NAME = "sl4"
    CHUNK_SIZE = 5

    # Thực thi transfer (decorator sẽ in dòng kết luận)
    transfer_csv_to_postgres(CSV_PATH, TABLE_NAME, engine, chunksize=CHUNK_SIZE)
    # Giải phóng engine
    engine.dispose()
