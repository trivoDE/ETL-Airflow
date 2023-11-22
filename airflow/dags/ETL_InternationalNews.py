from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
import requests
import pyodbc
from pandas import json_normalize
import time
from bs4 import BeautifulSoup
import re

def getNewsVnexpress():
    
    def get_article_publish_date_Vnexpress(url):
        response = requests.get(url)
        html_content = response.text
        soup = BeautifulSoup(html_content, "html.parser")

        date_elements = soup.find_all('span', class_='date')
        if date_elements:
            publish_dates = []
            for date_element in date_elements:
                date_string = date_element.text
                match = re.search(r'\d+/\d+/\d+, \d+:\d+', date_string)
                if match:
                    publish_date_str = match.group()
                    publish_date = datetime.strptime(publish_date_str, '%d/%m/%Y, %H:%M')
                    publish_dates.append(publish_date)
            return publish_dates

        return None

    url = "https://vnexpress.net/kinh-doanh/quoc-te"
    response = requests.get(url)
    html_content = response.text
    soup = BeautifulSoup(html_content, "html.parser")
    articles = soup.select('div.width_common.list-news-subfolder.has-border-right > article > div > a')

    data = []

    for article in articles:
        title = article.get('title')
        href = article.get('href')

        if article.img.get('data-src') is not None:
            img = article.img.get('data-src')
        else:
            img = article.img.get('src')

        subtitle_element = article.find_next('p', class_='description')
        subtitle = subtitle_element.text.strip() if subtitle_element else ''

        publish_dates = get_article_publish_date_Vnexpress(href)
        if publish_dates:
            publish_date = publish_dates[0]
        else:
            publish_date = datetime.now()

        data.append((publish_date, title, href, img, subtitle))
    columns = ['Date', 'Title', 'Href', 'Img', 'Subtitle']
    df = pd.DataFrame(data, columns=columns)

    return df

def getNewsVneconomy():
    
    url = "https://vneconomy.vn/the-gioi-kinh-te.htm"
    urls = "https://vneconomy.vn/"
    
    def get_article_publish_date_Vneconomy(url):
        response = requests.get(url)
        html_content = response.text
        soup = BeautifulSoup(html_content, "html.parser")

        date_element = soup.find(class_="detail__meta")
        if date_element:
            date_string = date_element.text.strip().split('|')[0].strip()
            publish_date = datetime.strptime(date_string, '%H:%M %d/%m/%Y')
            return publish_date

        return None

    response = requests.get(url)
    html_content = response.text
    soup = BeautifulSoup(html_content, "html.parser")
    articles = soup.find_all('article')

    data = []

    for article in articles:
        title_element = article.find('h3', class_="story__title")
        title = title_element.text.strip() if title_element else ''

        href_element = article.find('h3', class_="story__title").a
        href = urls + href_element['href'] if href_element and 'href' in href_element.attrs else ''

        img_element = article.find('img')
        img = img_element['data-src'] if img_element and 'data-src' in img_element.attrs else img_element['src'] if img_element else ''

        subtitle_element = article.find_next(class_="story__summary")
        subtitle = subtitle_element.text.strip() if subtitle_element else ''

        publish_date = get_article_publish_date_Vneconomy(href)

        data.append((publish_date, title, href, img, subtitle))
    columns = ['Date', 'Title', 'Href', 'Img', 'Subtitle']
    df = pd.DataFrame(data, columns=columns)

    return df

def getNewsCongthuong():
    
    url = "https://congthuong.vn/hoi-nhap-quoc-te/thong-tin-thuong-vu"

    def get_article_publish_date_Congthuong(url):
        response = requests.get(url)
        html_content = response.text
        soup = BeautifulSoup(html_content, "html.parser")

        date_element = soup.find('span', class_="format_time")
        if date_element:
            date_string = date_element.text.strip()
            publish_date = datetime.strptime(date_string, '%d/%m/%Y %H:%M')
            return publish_date

        return None

    response = requests.get(url)
    html_content = response.text
    soup = BeautifulSoup(html_content, "html.parser")
    articles = soup.find_all(class_="article")

    data = []
    encountered_titles = set()

    for article in articles:
        title_element = article.find('h3', class_="article-title")
        title = title_element.text.strip() if title_element else ''

        if title in encountered_titles:
            continue

        encountered_titles.add(title)

        href_element = article.find('a', class_="article-link f0")
        href = href_element['href'] if href_element and 'href' in href_element.attrs else ''

        img_element = article.find('img')
        img = img_element['data-src'] if img_element and 'data-src' in img_element.attrs else img_element['src'] if img_element else ''

        subtitle_element = article.find_next(class_="article-desc")
        subtitle = subtitle_element.text.strip() if subtitle_element else ''

        publish_date = get_article_publish_date_Congthuong(href)

        if publish_date is None:
            continue

        data.append((publish_date, title, href, img, subtitle))
        
    columns = ['Date', 'Title', 'Href', 'Img', 'Subtitle']
    df = pd.DataFrame(data, columns=columns)

    return df

def concatData(df1, df2, df3):
    combined_df = pd.concat([df1, df2, df3], ignore_index=True)
    return combined_df

def importDataframeToSqlServer(table_name, df):
    # Connect to the SQL Server database
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
    df.replace(to_replace=["'"], value='"', regex=True, inplace=True)

    # Insert the data into the table
    dem = 0
    for row in df.itertuples(index=False):
        
        values = "','".join(f"{col}" if isinstance(col, str) else str(col) for col in row)
        values = values.replace("NaT", "").strip()
        values = values.replace("'s", "").strip()
        values = values.replace(",'", ",N'").strip()
        
        sql = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES (N'{values}')"
        
        cursor.execute(sql)
        dem += 1
    
    cte = f'''WITH cte AS (
        SELECT 
            {', '.join(column_names)},
            ROW_NUMBER() OVER (
                PARTITION BY {', '.join(column_names)}
                ORDER BY {', '.join(column_names)}
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

    print(f'Import {table_name} {dem} rows')

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
# Định nghĩa DAG
with DAG(
    'ETL_InternationalNews',
    default_args=default_args,
    description='ETL_International_News_to_MSSQL',
    schedule_interval= "0 */8 * * *", #chạy 8h 1 lần
) as dag:

    # Task 1: 
    ExtractVnexpress= PythonOperator(
        task_id='Extract_News_in_Vnexpress',
        python_callable=getNewsVnexpress,
    )

    # Task 2:
    ExtractVneconomy = PythonOperator(
        task_id='Extract_News_in_Vneconomy',
        python_callable=getNewsVneconomy,
    )

    # Task 3:
    ExtractCongthuong = PythonOperator(
        task_id='Extract_News_in_Congthuong',
        python_callable=getNewsCongthuong,
    )

    # Task 4:
    TransData = PythonOperator(
        task_id='Concat_Data',
        python_callable=concatData,
        op_args=[ExtractVnexpress.output,ExtractVneconomy.output,ExtractCongthuong.output]
    )


    # Task 5:
    LoadData = PythonOperator(
        task_id='Import_BCTC_TO_SQL',
        python_callable=importDataframeToSqlServer,
        op_args=['[macroEconomic].[dbo].[TinTucQuocTe]',TransData.output]
    )

    # Xếp các task theo thứ tự
ExtractVnexpress >> TransData
ExtractVneconomy >> TransData
ExtractCongthuong >> TransData
TransData >> LoadData