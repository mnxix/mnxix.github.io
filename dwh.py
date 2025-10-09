import pandas as pd
from sqlalchemy import create_engine

# Kết nối tới PostgreSQL trong Docker
engine = create_engine('postgresql://postgres:123456@localhost:5432/studentdb')

# Đọc CSV
df = pd.read_csv('dsl.csv')
print(df.head())

# Ghi vào PostgreSQL
df.to_sql('students', engine, index=False, if_exists='replace')
print("✅ Đã ghi dữ liệu vào bảng 'students'")
