# Import các thư viện và modules cần thiết
from datetime import datetime, timedelta
from vnstock import *
from airflow import DAG
from airflow.operators.python import PythonOperator

# Hàm lấy dữ liệu từ vnstock
def get_vnstock_data():
    df = company_overview('TCB')
    return df

# Hàm lưu dữ liệu vào CSV
def save_data_to_csv(df):
    
    df.to_excel('./example/tcb.xlsx', index=False)  # Thay đổi đường dẫn tới nơi bạn muốn lưu trữ dữ liệu

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
    'vnstock_dag',
    default_args=default_args,
    description='Fetch data from vnstock and save to CSV',
    schedule_interval=timedelta(days=1),
) as dag:

    # Định nghĩa PythonOperator để chạy hàm lấy dữ liệu
    fetch_vnstock_data = PythonOperator(
        task_id='fetch_vnstock_data',
        python_callable=get_vnstock_data,
    )

    # Định nghĩa PythonOperator để chạy hàm lưu dữ liệu
    save_to_csv = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_data_to_csv,
        op_args=[fetch_vnstock_data.output],  # Truyền kết quả của fetch_vnstock_data vào save_data_to_csv
    )

# Đặt sự phụ thuộc giữa các task
fetch_vnstock_data >> save_to_csv
