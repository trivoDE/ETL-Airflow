from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
import requests
import pyodbc
from pandas import json_normalize
import time
import yfinance as yf



def ExtractDataWorldIndices():
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date =(datetime.now()-timedelta(7)).strftime("%Y-%m-%d")
    index_list = ["^GSPC", "^FTSE", "^KS11", "^DJI", "^N225", "000001.SS", "^GDAXI"]


    # Tạo một DataFrame để lưu trữ dữ liệu
    df_combined = pd.DataFrame()

    # Lặp qua danh sách chỉ số chứng khoán
    for index_code in index_list:
        # Lấy dữ liệu từ yfinance
        index_data = yf.Ticker(index_code).history(start=start_date, end=end_date)
        
        # Thêm cột mã chứng khoán
        index_data["Code"] = index_code
        
        # Thêm dữ liệu vào DataFrame tổng hợp
        df_combined = pd.concat([df_combined, index_data])

    return df_combined

def TransformDataWorldIndices(dfraw):

    df = dfraw.reset_index()

    df["Name"] = df["Code"].map({
        "^GSPC": "S&P 500",
        "^FTSE": "FTSE 100",
        "^KS11": "KOSPI",
        "^DJI": "Dow Jones",
        "^N225": "Nikkei 225",
        "000001.SS": "Shanghai",
        "^GDAXI": "DAX"
    })

    df['Date'] = (df['Date'].astype(str)).str[:10]

    df = df.replace('000001.SS', '^SSEC')
    df = df.drop(columns=['Dividends', 'Stock Splits'])

    df['id'] = df['Code'] + df['Date'].str.replace('-', '')

    df["Code"] = df["Code"].str.replace('^', '')
    df["id"] = df["id"].str.replace('^', '')

    df = df.astype({'Open': float}).round(2)
    df = df.astype({'High': float}).round(2)
    df = df.astype({'Low': float}).round(2)
    df = df.astype({'Close': float}).round(2)
    df.rename(columns={'Open': 'openPrice', 'High': 'highPrice', 'Low': 'lowPrice', 'Close': 'closePrice', 'Date': 'date', 'Code': 'code', 'Name': 'name' }, inplace=True)

    return df

def LoadDataWorldIndices(table_name, df):
    conn_str = ("Driver={ODBC Driver 17 for SQL Server};"
                "Server=192.168.17.22,1433;"
                "Database=marketInfor;"
                "UID=admin2;"
                "PWD=123beta456;"
                "charset=UTF8")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Define the column names
    column_names = list(df.columns)

    # Check if 'id' column exists and is not null
    if 'id' not in column_names or pd.isna(df['id'].iloc[0]):
        print("Error: 'id' column not found or contains null values.")
        return

    # Insert or update the data into the table
    for row in df.itertuples(index=False):
        id_value = getattr(row, 'id')
        sql_check = f"SELECT COUNT(id) FROM {table_name} WHERE id = '{id_value}'"
        cursor.execute(sql_check)
        row_count = cursor.fetchone()[0]

        if row_count > 0:
            # Update the row
            set_values = ", ".join(f"{col} = N'{getattr(row, col)}'" for col in column_names)
            sql_update = f"UPDATE {table_name} SET {set_values} WHERE id = '{id_value}'"
            #print(sql_update)
            cursor.execute(sql_update)
        else:
            # Insert a new row
            values = "','".join(f"{col}" if isinstance(col, str) else str(col) for col in row)
            values = values.replace("NaT", "").strip()
            #values = values.replace("'s", "").strip()
            values = values.replace(",'", ",N'").strip()
            sql_insert = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES (N'{values}')"
            #print(sql_insert)
            cursor.execute(sql_insert)

    # Remove duplicate rows based on all columns
    cte = f'''WITH cte AS (
        SELECT 
            {', '.join(column_names)},
            ROW_NUMBER() OVER (
                PARTITION BY {', '.join(column_names)}
                ORDER BY (SELECT NULL)
            ) row_num
        FROM {table_name}
    )
    DELETE FROM cte
    WHERE row_num > 1;
    '''

    cursor.execute(cte)
    conn.commit()
    cursor.close()
    conn.close()

# Định nghĩa các tham số mặc định cho DAG
default_args = {
    'owner': 'MinhTri',
    'depends_on_past': True,
    'start_date': datetime(2023, 11, 21, 9, 0, 0)
    }

with DAG(
    'ETL_WorldIndices',
    default_args=default_args,
    description='ETL_Commodity_to_MSSQL',
    schedule_interval= "0 */8 * * *", #chạy 8h 1 lần

) as dag:

    # Task 1: Extract
    ExtractWorldIndices= PythonOperator(
        task_id='Extract_WorldIndices',
        python_callable=ExtractDataWorldIndices,
        
    )
    # Task 2:
    TransformWorldIndices= PythonOperator(
        task_id='Transform_WorldIndices',
        python_callable=TransformDataWorldIndices,
        op_args=[ExtractWorldIndices.output]
    )

    LoadWorldIndices = PythonOperator(
        task_id='Load_WorldIndices',
        python_callable=LoadDataWorldIndices,
        op_args=['[macroEconomic].[dbo].WorldIndices',TransformWorldIndices.output]
    )
ExtractWorldIndices >> TransformWorldIndices >> LoadWorldIndices