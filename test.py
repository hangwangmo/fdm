import pymysql
import json
import FinanceDataReader as fdr

bas_dt = '2021-11-26'
config_path = '/home/gwangmo/shared_directory/study/python/market_data/config/config.json'

with open(config_path) as f:
    config = json.load(f)
    
df_krx_stock_info = fdr.StockListing('KRX')

conn = pymysql.connect(host=config['DATABASE']['HOST'], user=config['DATABASE']['USERNAME']
                       , password=config['DATABASE']['PASSWORD'], db=config['DATABASE']['DB_NAME']
                       , charset=config['DATABASE']['CHARSET']) 

cursor = conn.cursor() 

for stock_info in df_krx_stock_info.itertuples():
    sql = """
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
                   , 'jupyter notebook'
                   , now()
                   , null
                   );
            """ 
    
    print(stock_info[1])
    print(stock_info[2])
    print(stock_info[3])
    print(stock_info[4])
    print(stock_info[5])
    print(stock_info[6])
    print(stock_info[7])
    print(stock_info[8])
    print(stock_info[9])
    print(stock_info[10])

    cursor.execute(sql,(bas_dt,stock_info[1],stock_info[3]
                        ,stock_info[3],stock_info[4],stock_info[5]
                        ,stock_info[6],stock_info[7],stock_info[8]
                        ,stock_info[9],stock_info[10])) 

conn.commit() 
conn.close() 