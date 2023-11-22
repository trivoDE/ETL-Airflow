from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
import requests
import pyodbc
from pandas import json_normalize
import time


def getDataHangHoa():
    apis = {
        'brent': "https://markets.tradingeconomics.com/chart?s=co1:com&span=1y&securify=new&url=/commodity/crude-oil&AUTH=sesame&ohlc=0",
        'wti': "https://markets.tradingeconomics.com/chart?s=cl1:com&interval=1d&span=1y&securify=new&url=/commodity/crude-oil&AUTH=sesame&ohlc=0",
        'gold': "https://markets.tradingeconomics.com/chart?s=xauusd:cur&interval=1d&span=1y&securify=new&url=/commodity/crude-oil&AUTH=sesame&ohlc=0",
        'gas': "https://markets.tradingeconomics.com/chart?s=ng1:com&interval=1d&span=1y&securify=new&url=/commodity/crude-oil&AUTH=sesame&ohlc=0",
        'steel': "https://markets.tradingeconomics.com/chart?s=jbp:com&interval=1d&span=1y&securify=new&url=/commodity/crude-oil&AUTH=sesame&ohlc=0",
        'sugar': "https://markets.tradingeconomics.com/chart?s=sb1:com&interval=1d&span=1y&securify=new&url=/commodity/crude-oil&AUTH=sesame&ohlc=0",
        'rubber': "https://markets.tradingeconomics.com/chart?s=jn1:com&interval=1d&span=1y&securify=new&url=/commodity/crude-oil&AUTH=sesame&ohlc=0",
        'gasoline':"https://markets.tradingeconomics.com/chart?s=xb1:com&interval=1d&span=1y&securify=new&url=/commodity/crude-oil&AUTH=sesame&ohlc=0"
    }

    # Gửi yêu cầu API và tổng hợp dữ liệu
    result = []
    for key, api in apis.items():
        response = requests.get(api).json()
        for series in response['series']:
            for entry in series['data']:
                result.append({
                    #'symbol': series['symbol'],
                    'name': series['shortname'],
                    #'fullname': series['full_name'],
                    'api': key,
                    'lastUpdated': pd.to_datetime(entry['x'], unit='ms'),
                    'price': entry['y']
                })
            time.sleep(1)

    df = pd.DataFrame(result)
    df['id'] = df['api'] + df['lastUpdated'].astype(str).str.replace('-', '')
    df = df.drop(columns=['api'])
    unit_mapping = {
        'Brent': 'USD/Bbl',
        'Crude Oil': 'USD/Bbl',
        'Gold': 'USD/t.oz',
        'Natural gas': 'USD/MMBtu',
        'Steel': 'CNY/T',
        'Sugar': 'USD/Lbs',
        'Rubber': 'USD Cents/Kg',
        'Gasoline': 'USD/Gallon'
    }
    df['unit'] = df['name'].map(unit_mapping)

    df['name'] = df['name'].map({
        'Brent': 'Dầu Brent',
        'Crude Oil': 'Dầu Thô',
        'Gold': 'Vàng',
        'Natural gas': 'Khí Gas',
        'Steel': 'Thép',
        'Sugar': 'Đường',
        'Rubber': 'Cao su',
        'Gasoline': 'Xăng'
    })

    df['lastUpdated'] = pd.to_datetime(df['lastUpdated'])
    df = df.sort_values(by=['name', 'lastUpdated'])

    periods = {'1D': 1, '5D': 5, '1M': 20, '3M': 60, '6M': 120, '1Y': 240}
    for period_name, period_value in periods.items():
        df[f'valueChange{period_name}'] = df.groupby('name')['price'].diff(period_value)
        df[f'percentChange{period_name}'] = (df[f'valueChange{period_name}'] / df['price']) * 100
        df[f'change{period_name}'] = df[f'valueChange{period_name}'].round(2).astype(str) + ' (' + (df[f'percentChange{period_name}']).round(2).astype(str) + '%)'
        df = df.drop(columns=[f'valueChange{period_name}',f'percentChange{period_name}'])



    # Sắp xếp DataFrame theo 'shortname', 'time'
    df = df.sort_values(by=['name', 'lastUpdated'])

    # Tính giá trị đầu tiên của mỗi tháng và cuối cùng của năm
    df['first_day_of_month'] = df.groupby(['name', df['lastUpdated'].dt.to_period('M')])['lastUpdated'].transform('first')
    df['first_day_of_year'] = df.groupby(['name', df['lastUpdated'].dt.to_period('Y')])['lastUpdated'].transform('first')

    first_value_of_month = df.groupby(['name', 'first_day_of_month'])['price'].transform('first')
    last_value_of_year = df.groupby(['name', 'first_day_of_year'])['price'].transform('first')

    # Tính toán MTD và YTD
    df['MTD'] = df['price'] - first_value_of_month
    df['YTD'] = df['price'] - last_value_of_year
    df['percent_change_MTD'] = (df['MTD'] / df['price']) * 100
    df['percent_change_YTD'] = (df['YTD'] / df['price']) * 100
    df[f'changeMTD'] = df[f'MTD'].round(2).astype(str) + ' (' + (df['percent_change_MTD']).round(2).astype(str) + '%)'
    df[f'changeYTD'] = df[f'YTD'].round(2).astype(str) + ' (' + (df['percent_change_YTD']).round(2).astype(str) + '%)'

    df = df.drop(columns=['MTD','YTD','percent_change_MTD','percent_change_YTD','first_day_of_month','first_day_of_year'])
    df.replace('nan (nan%)','',inplace=True)
    return df

def importUpdateSql(table_name, df):
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
            print(sql_update)
            cursor.execute(sql_update)
        else:
            # Insert a new row
            values = "','".join(f"{col}" if isinstance(col, str) else str(col) for col in row)
            values = values.replace("NaT", "").strip()
            #values = values.replace("'s", "").strip()
            values = values.replace(",'", ",N'").strip()
            sql_insert = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES (N'{values}')"
            print(sql_insert)
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
    'start_date': datetime(2023, 11, 21, 9, 0, 0),  # Đặt thời gian bắt đầu lúc 5h chiều
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['trivominhit@gmail.com'],  # Thay bằng địa chỉ email của bạn
    'email_on_failure': True,   # Gửi email khi có lỗi
    'email_on_retry': True,     # Gửi email khi thử lại
    'email_on_success': True    # Gửi email khi task hoàn thành thành công
}

with DAG(
    'ETL_Commodity',
    default_args=default_args,
    description='ETL_Commodity_to_MSSQL',
    schedule_interval= "0 */8 * * *", #chạy 8h 1 lần

) as dag:

    # Task 1: 
    ExtractCommodity= PythonOperator(
        task_id='Extract_Commodity',
        python_callable=getDataHangHoa,
    )
    # Task 2:
    LoadDataCommodity = PythonOperator(
        task_id='Import_Commodity_To_SQL',
        python_callable=importUpdateSql,
        op_args=['[macroEconomic].[dbo].[HangHoa]',ExtractCommodity.output]
    )
ExtractCommodity >> LoadDataCommodity