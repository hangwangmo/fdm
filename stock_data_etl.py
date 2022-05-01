import pymysql
import json
import FinanceDataReader as fdr
import pandas as pd
from sqlalchemy import create_engine

import sys

if len(sys.argv) < 3:
    print("Insufficient arguments, [Usage] > python stock_data_etl.py [yyyymmdd] [config file path]")
    sys.exit()

bas_dt = sys.argv[1]
config_path = sys.argv[2]

with open(config_path) as f:
    config = json.load(f)
    
df_krx_stock_info = fdr.StockListing('KRX')
df_krx_stock_info = df_krx_stock_info.where((pd.notnull(df_krx_stock_info)), None)
df_krx_stock_info['ListingDate'] = df_krx_stock_info['ListingDate'].astype('str')
df_krx_stock_info.replace({'NaT': None}, inplace=True)

conn = pymysql.connect(host=config['DATABASE']['HOST'], user=config['DATABASE']['USERNAME']
                       , password=config['DATABASE']['PASSWORD'], db=config['DATABASE']['DB_NAME']
                       , charset=config['DATABASE']['CHARSET']) 

cursor = conn.cursor() 

delete_sql = """
        delete from market_data.stock_info where bas_dt = %s
        """
cursor.execute(delete_sql,(bas_dt))

for stock_info in df_krx_stock_info.itertuples():

    insert_sql = """
            insert into market_data.stock_info ( bas_dt
                                               , symbol
                                               , market
                                               , name
                                               , sector
                                               , industry
                                               , listing_date
                                               , settle_month
                                               , representative
                                               , homepage
                                               , region
                                               , worker
                                               , create_datetime
                                               , modified_datetime
                                               )
            values ( %s
                   , %s
                   , %s
                   , %s
                   , %s
                   , %s
                   , %s
                   , %s
                   , %s
                   , %s
                   , %s
                   , 'dbtest.py'
                   , now()
                   , null
                   );
            """ 

    cursor.execute(insert_sql,(bas_dt,stock_info[1],stock_info[2]
                        ,stock_info[3],stock_info[4],stock_info[5]
                        ,stock_info[6],stock_info[7],stock_info[8]
                        ,stock_info[9],stock_info[10])) 

    try:
        df_krx_stock_price = fdr.DataReader(stock_info[1], bas_dt, bas_dt)

        for stock_price in df_krx_stock_price.itertuples():
            insert_price_sql = """
                    insert into market_data.stock_price ( bas_dt
                                                        , symbol
                                                        , open
                                                        , high
                                                        , low
                                                        , close
                                                        , volume
                                                        , `change`
                                                        , worker
                                                        , create_datetime
                                                        , modified_datetime
                                                        )
                    values ( %s
                        , %s
                        , %s
                        , %s
                        , %s
                        , %s
                        , %s
                        , %s
                        , 'dbtest.py'
                        , now()
                        , null
                        );
                    """
            # print(bas_dt)
            # print(stock_info[1])
            # print(stock_price[1])
            # print(stock_price[2])
            # print(stock_price[3])
            # print(stock_price[4])
            # print(stock_price[5])
            # print(stock_price[6])

            cursor.execute(insert_price_sql,(bas_dt,stock_info[1],stock_price[1]
                                ,stock_price[2],stock_price[3],stock_price[4]
                                ,stock_price[5],stock_price[6])) 
    except:
        # print("%s는 종가가 없습니다.",stock_info[1])
        continue

conn.commit() 
conn.close() 