# Import các thư viện và modules cần thiết
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pyodbc
import pandas as pd

# Kết nối với cơ sở dữ liệu SQL Server
def connect_to_db():
    conn_str = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=192.168.17.22,1433;"
        "Database=financialReport;"
        "UID=admin2;"
        "PWD=123beta456;"
        "charset=UTF8"
    )
    conn = pyodbc.connect(conn_str)
    return conn

# Hàm lấy dữ liệu từ cơ sở dữ liệu SQL Server
def get_data_from_db():
    conn = connect_to_db()
    query = "SELECT TOP (10) * FROM [financialReport].[dbo].[financialReportV2] WHERE code ='VCB'"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Hàm lưu dữ liệu vào Excel
def save_data_to_excel(df):
    df.to_excel('./example/hpg.xlsx', index=False)  # Thay đổi đường dẫn tới nơi bạn muốn lưu trữ dữ liệu

# Định nghĩa các tham số mặc định cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Định nghĩa DAG
with DAG(
    'sql_excel',
    default_args=default_args,
    description='Fetch data from sql and save to CSV',
    schedule_interval=timedelta(days=1),
) as dag:

    # Định nghĩa PythonOperator để chạy hàm lấy dữ liệu từ cơ sở dữ liệu
    fetch_data_from_db = PythonOperator(
        task_id='fetch_data_from_db',
        python_callable=get_data_from_db,
    )

    # Định nghĩa PythonOperator để chạy hàm lưu dữ liệu vào Excel
    save_to_excel = PythonOperator(
        task_id='save_to_excel',
        python_callable=save_data_to_excel,
        op_args=[fetch_data_from_db.output],  # Truyền kết quả của fetch_data_from_db vào save_data_to_excel
    )

# Đặt sự phụ thuộc giữa các task
fetch_data_from_db >> save_to_excel
