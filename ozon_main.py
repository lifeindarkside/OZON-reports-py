import requests
import json
import pandas as pd
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, event
from urllib.parse import quote_plus
import sys
import datetime as DT

#get your client id and api key from ozon seller.
hdrs = {"Client-Id": "clien-id", "Api-Key": "api-key"}

#enter date when you have orders (get information about every order in every day)
startdate = DT.date(2022,3,15)
enddate = DT.date(2022,3,31)

def date_generate(startdate,enddate):
    date = startdate
    dates = [startdate]
    while date < enddate:
        date += DT.timedelta(days=1)
        dates.append(date)
    return dates
    
def dates_transform_start(i):
    date_format = '%Y-%m-%d'
    start_dt = i.strftime(date_format)
    start_dt = start_dt + "T00:00:00.000Z"
    return start_dt
    
def dates_transform_end(i):
    date_format = '%Y-%m-%d'
    end_dt = i + timedelta(days=0)
    end_dt = end_dt.strftime(date_format)
    end_dt = end_dt + "T00:00:00.000Z"
    return end_dt
    
def get_JSON(start_dt,end_dt,hdrs):
    body = {
    "filter": {
        "date": {
            "from": start_dt,
            "to": end_dt
        },
        "operation_type": [],
        "posting_number": "",
        "transaction_type": "all"
    },
    "page": 1,
    "page_size": 500
    }
    body = json.dumps(body)

    report_data = requests.post("https://api-seller.ozon.ru/v3/finance/transaction/list", headers=hdrs, data=body)

    return report_data
    

def get_json_data(hdrs,page,start_dt,end_dt):
    body = {
        "filter": {
            "date": {
                "from": start_dt,
                "to": end_dt
            },
            "operation_type": [],
            "posting_number": "",
            "transaction_type": "all"
        },
        "page": page,
        "page_size": 500
    }
    body = json.dumps(body)
    report_data = requests.post("https://api-seller.ozon.ru/v3/finance/transaction/list", headers=hdrs, data=body)
    json_report_data = report_data.json()
    return json_report_data

def parse_json_main(json_report_data):
    data_to_df = json_report_data["result"]["operations"]
    df = pd.json_normalize(data_to_df, errors='ignore', sep='.', max_level=None)
    df = df.drop(columns='items')
    df = df.drop(columns='services')
    return df
    
    
def parse_json_services(json_report_data):
    data_to_df = json_report_data["result"]["operations"]
    df = pd.json_normalize(data_to_df, 'services',
                                 ['operation_id', 'operation_type', 'operation_date', ['posting','posting_number'], ['posting', 'order_date']],
                                 errors='ignore')
    return df
    
def parse_json_items(json_report_data):
    data_to_df = json_report_data["result"]["operations"]
    df = pd.json_normalize(data_to_df, 'items',
                              ['operation_id', 'operation_type', 'operation_date', ['posting','posting_number'], ['posting', 'order_date']],
                              errors='ignore')
    return df
    
def transfer_to_SQL(engine,df,tablename):
    df.to_sql(tablename, engine, index=False, if_exists='append', chunksize=None)
    
def engine():
    #replace SERVERNAME, DATABASE, login and password to transfer data in SQL server
    conn = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=SERVERNAME;DATABASE=DBNAME;UID=login;PWD=password"
    quoted = quote_plus(conn)
    new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
    engine = create_engine(new_con, fast_executemany=True)
    
    
def parse_json_to_SQL(hdrs,startdate,enddate):
    dates = date_generate(startdate,enddate)
    engine_con = engine()
    for i in dates:
        start_dt = dates_transform_start(i)
        end_dt = dates_transform_end(i)
        json_report_data = get_JSON(start_dt,end_dt,hdrs)
        if json_report_data.ok:
            json_report_data = json_report_data.json()
            m_pages = json_report_data["result"]["page_count"]
        else:
            print('JSON is empty')
        for page in range(1,m_pages+1):
            time.sleep(10)
            sys.stdout.write("\rcurrent page:" + str(page) + " from " + str(m_pages))
            json_report_data = get_json_data(hdrs,page,start_dt,end_dt)
            df_main = parse_json_main(json_report_data)
            transfer_to_SQL(engine_con,df_main,'Main')
            #print('Main send OK')
            df_services = parse_json_services(json_report_data)
            transfer_to_SQL(engine_con,df_services,'Services')
            #print('Sevices send OK')
            df_items = parse_json_items(json_report_data)
            transfer_to_SQL(engine_con,df_items,'Items')
            #print('Items send OK')
            time.sleep(30)
        print('Dates done: start date - ',start_dt,' end date - ', end_dt)
        time.sleep(30)
        
  parse_json_to_SQL(hdrs,startdate,enddate)
    return engine
