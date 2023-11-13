from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
import requests
import pyodbc
from pandas import json_normalize
import time

# Định nghĩa các hàm



        
def LoadBCTC(pathJsonData):
    
    conn_str = ("Driver={ODBC Driver 17 for SQL Server};"
                "Server=192.168.17.22,1433;"
                "Database=financialReport;"
                "UID=admin2;"
                "PWD=123beta456;"
                "charset=UTF8")
    conn = pyodbc.connect(conn_str)

    cursor = conn.cursor()

    df = pd.read_json(pathJsonData, lines=True)
    
    df.replace(np.nan, '', inplace=True)

    stt = 0
    for row in df.itertuples():
        print(f'Import {stt} {row.idBCTC}')
        cursor.execute('''
                    INSERT INTO [zAtisss].[dbo].[BCTC_fireant]
                    ([idBCTC], [code], [id], [parentID], 
                    [period], [year], [quarter],
                    [name], [value], 
                    [type], [level],[yearQuarter])
                    VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    row.idBCTC, row.code,row.id,row.parentID,
                    row.period, row.year, row.quarter,
                    row.name,  row.value,   
                    row.type ,row.level, row.yearQuarter
                    )
        stt=stt+1
    log= f'Import BCTC_fireant {stt} rows'
    with open('./example/logLoadBCTC.txt', 'w') as file:
        file.write(log)
    conn.commit()

    cursor.close()
    return stt

def deleteBCTC_fireant():
    
    conn_str = ("Driver={ODBC Driver 17 for SQL Server};"
                "Server=192.168.17.22,1433;"
                "Database=financialReport;"
                "UID=admin2;"
                "PWD=123beta456;"
                "charset=UTF8")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
 
    cursor.execute('''delete [zAtisss].[dbo].[BCTC_fireant]''')
    conn.commit()
    cursor.close()
    conn.close()

def getBCTC_quy(ticker):
    dfall_list = []
    
    #for z in range(1,5):
    for z in range(1,5):
        try:
            ### quý
            url = f'https://restv2.fireant.vn/symbols/{ticker}/full-financial-reports?type={z}&year=2023&quarter=5&limit=43'
            headers = {
                "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg",
                "Accept": "application/json"
            }

            response = requests.get(url, headers=headers)
            data = response.json()
            time.sleep(3)
            periods = set()
            for obj in data:
                values = obj.get('values', [])
                for value in values:
                    period = value.get('period')
                    periods.add(period)
            num_periods = len(periods)
            #print("Số lượng period là:", num_periods)

            df = pd.DataFrame(data)

            df_list = []
            for i in range(0, num_periods):
                df_new = pd.DataFrame()
                df_new['id'] = df['id']
                df_new['name'] = df['name']
                df_new['parentID'] = df['parentID']
                df_new['expanded'] = df['expanded']
                df_new['level'] = df['level']
                df_new['field'] = df['field']
                df_new['period'] = df['values'].apply(lambda x: x[i]['period'])
                df_new['year'] = df['values'].apply(lambda x: x[i]['year'])
                df_new['quarter'] = df['values'].apply(lambda x: x[i]['quarter'])
                df_new['value'] = df['values'].apply(lambda x: x[i]['value'])

                df_list.append(df_new)

            df_combined = pd.concat(df_list, ignore_index=True)
            if z == 1:
                df_combined['type'] = 'CDKT' 
                #print(ticker+' CDKT '+str(num_periods))
            elif z == 2:
                df_combined['type'] = 'KQKD' 
                #print(ticker+' KQKD '+str(num_periods))
            elif z == 3:
                df_combined['type'] = 'LCTT'
                #print(ticker+' LCTT '+str(num_periods))
            elif z == 4:
                df_combined['type'] = 'LCGT'
                #print(ticker+' LCGT '+str(num_periods))

            dfall_list.append(df_combined)
        except:
            pass
    dfall_combined = pd.concat(dfall_list, ignore_index=True)
    
    dfall_combined['ticker']=ticker
    
    dfall_combined['idBCTC'] = dfall_combined['ticker'].astype(str) + dfall_combined['type'].astype(str) + dfall_combined['year'].astype(str) + dfall_combined['quarter'].astype(str) + dfall_combined['id'].astype(str)
    time.sleep(3)
    
    dfall_combined = dfall_combined.rename(columns={'ticker': 'code'})
    
    dfall_combined['yearQuarter'] = dfall_combined['year'].astype(str)+dfall_combined['quarter'].astype(str)
    
    return dfall_combined

def getBCTC_nam(ticker):
    dfall_list = []
    
    #for z in range(1,5):
    for z in range(1,5):
        try:
            ### quý
            url = f'https://restv2.fireant.vn/symbols/{ticker}/full-financial-reports?type={z}&year=2023&quarter=0&limit=10'
            headers = {
                "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg",
                "Accept": "application/json"
            }

            response = requests.get(url, headers=headers)
            data = response.json()
            time.sleep(3)
            periods = set()
            for obj in data:
                values = obj.get('values', [])
                for value in values:
                    period = value.get('period')
                    periods.add(period)
            num_periods = len(periods)
            #print("Số lượng period là:", num_periods)

            df = pd.DataFrame(data)

            df_list = []
            for i in range(0, num_periods):
                df_new = pd.DataFrame()
                df_new['id'] = df['id']
                df_new['name'] = df['name']
                df_new['parentID'] = df['parentID']
                df_new['expanded'] = df['expanded']
                df_new['level'] = df['level']
                df_new['field'] = df['field']
                df_new['period'] = df['values'].apply(lambda x: x[i]['period'])
                df_new['year'] = df['values'].apply(lambda x: x[i]['year'])
                df_new['quarter'] = df['values'].apply(lambda x: x[i]['quarter'])
                df_new['value'] = df['values'].apply(lambda x: x[i]['value'])

                df_list.append(df_new)

            df_combined = pd.concat(df_list, ignore_index=True)
            if z == 1:
                df_combined['type'] = 'CDKT' 
                #print(ticker+' CDKT '+str(num_periods))
            elif z == 2:
                df_combined['type'] = 'KQKD' 
                #print(ticker+' KQKD '+str(num_periods))
            elif z == 3:
                df_combined['type'] = 'LCTT'
                #print(ticker+' LCTT '+str(num_periods))
            elif z == 4:
                df_combined['type'] = 'LCGT'
                #print(ticker+' LCGT '+str(num_periods))

            dfall_list.append(df_combined)
        except:
            pass
    dfall_combined = pd.concat(dfall_list, ignore_index=True)
    
    dfall_combined['ticker']=ticker
    
    dfall_combined['idBCTC'] = dfall_combined['ticker'].astype(str) + dfall_combined['type'].astype(str) + dfall_combined['year'].astype(str) + dfall_combined['quarter'].astype(str) + dfall_combined['id'].astype(str)
    time.sleep(3)
    
    dfall_combined = dfall_combined.rename(columns={'ticker': 'code'})
    
    dfall_combined['yearQuarter'] = dfall_combined['year'].astype(str)+dfall_combined['quarter'].astype(str)
    
    return dfall_combined
    
def GetBCTC(ticker):
    dfquy = getBCTC_quy(ticker)
    dfnam = getBCTC_nam(ticker)
    merged_df = pd.concat([dfquy, dfnam], ignore_index=True)
    unique_df = merged_df.drop_duplicates()
    unique_df = unique_df[unique_df['year'] >= 2013]
    return unique_df


    
def listticker():
    url = 'https://finfo-api.vndirect.com.vn/v4/stocks?q=type:stock~status:~floor:HOSE,HNX,UPCOM&size=9999'
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    data = requests.get(url, headers=headers).json()
    df = json_normalize(data['data'])
    #df=df.rename(columns={"industryCode": "indexCode"})
    df_sorted = df.sort_values(by='code', ascending=True)
    return df_sorted['code']


def ExtractTransformBCTC():
    lticker = listticker()
    log=''
    dem=0
    df_list = []
    for i in lticker:
        try:
            dfBCTC =GetBCTC(i)

            log=log + f'Get {i} {len(dfBCTC)} rows\n'

            df_list.append(dfBCTC)
            print(f'{dem}. Get {i} {len(dfBCTC)} rows\n')
        except Exception as e:
            log = log + f'{dem} {i} error {e}\n'
        dem=dem+1
        time.sleep(3)
        
    combined_df = pd.concat(df_list, ignore_index=True)

    json_filename = './example/dataBCTC.json'
    combined_df.to_json(json_filename, orient='records', lines=True)

    log = log + f'Sum {len(combined_df)} rows'
    with open('./example/logExtractTransformBCTC.txt', 'w') as file:
        file.write(log)

    return json_filename





# Định nghĩa các tham số mặc định cho DAG
default_args = {
    'owner': 'MinhTri',
    'depends_on_past': True,
    'start_date': datetime(2023, 11, 1, 1, 0, 0),  # Đặt thời gian bắt đầu lúc 4h chiều
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['trivominhit@gmail.com'],  # Thay bằng địa chỉ email của bạn
    'email_on_failure': True,   # Gửi email khi có lỗi
    'email_on_retry': True,     # Gửi email khi thử lại
    'email_on_success': True    # Gửi email khi task hoàn thành thành công
}


# Định nghĩa DAG
with DAG(
    'ETL_FinancialReport',
    default_args=default_args,
    description='Import Financial Report to MSSQL',
    schedule_interval='@monthly'  ,
) as dag:

    # Task 1: Xóa dữ liệu cũ
    DeleteOldData = PythonOperator(
        task_id='Delete_BCTC_OLD',
        python_callable=deleteBCTC_fireant,
    )

    # Task 2: Import dữ liệu mới
    ExtractTransformdata = PythonOperator(
        task_id='Extract_Transform_BCTC',
        python_callable=ExtractTransformBCTC,
    )
    # Task 5: Lưu log
    LoadData = PythonOperator(
        task_id='Import_BCTC_TO_SQL',
        python_callable=LoadBCTC,
        op_args=[ExtractTransformdata.output]
    )

    # Xếp các task theo thứ tự
DeleteOldData >> ExtractTransformdata >> LoadData
