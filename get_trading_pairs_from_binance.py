from binance_config import api_secret
from binance_config import api_key
from binance.client import Client
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import os
import pprint
import time
from pathlib import Path
import sqlite3
def get_trading_pairs_from_binance():
    client=Client(api_key=api_key,api_secret=api_secret)
    # symbol='DOTUSDT'
    # interval='1d'
    # client.KLINE_INTERVAL_1DAY
    # klines=client.get_historical_klines(symbol,interval,"1 Jan,2021")
    # klines_df=pd.DataFrame(klines)
    # # create colums name
    # klines_df.columns = ['open_time' , 'open' , 'high' , 'low' , 'close' ,
    #                      'volume' , 'close_time' , 'qav' , 'num_trades' ,
    #                 'taker_base_vol' , 'taker_quote_vol', 'ignore']
    # klines_df.index=[dt.datetime.fromtimestamp(x/1000.0) for x in klines_df.open_time]
    # print(klines_df.head(3000).to_string())
    # klines_df=klines_df.astype(float)
    # # plt.plot(klines_df['close'])
    # # #plt.show()
    # # plt.savefig(f'{symbol}'+'.png')
    # # plt.close()

    # get list of all trading pairs from the binance exchange into a list
    exchange_info=client.get_exchange_info()
    #pprint.pprint(exchange_info)
    binance_symbols=[]
    for s in exchange_info['symbols']:
        pprint.pprint(s['symbol'])
        binance_symbols.append(s['symbol'])
        #time.sleep(3)

    #insert the list of trading pairs into a database

    path_to_databases=os.path.join(os.getcwd(),"datasets","sql_databases")
    Path(path_to_databases).mkdir(parents=True, exist_ok=True)
    connection=sqlite3.connect(os.path.join(os.getcwd(),"datasets",
                                            "sql_databases","binance_trading_pairs.db"))
    cur=connection.cursor()
    binance_symbols_df=pd.DataFrame(binance_symbols)
    print(binance_symbols_df)
    binance_symbols_df.columns=["trading_pair"]
    print ( binance_symbols_df )
    binance_symbols_df.to_sql("trading_pairs_tickers",connection,if_exists = 'replace')



get_trading_pairs_from_binance()