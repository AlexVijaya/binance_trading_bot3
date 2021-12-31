import websocket,json,pprint,talib
import numpy as np
from binance.client import Client
from binance.enums import *
import matplotlib.pyplot as plt
import configparser
import binance_config

socket_var="wss://stream.binance.com:9443/ws/ethusdt@kline_1m"
def binance_trading_bot(socket_var):
    client=Client(api_key = binance_config.api_key, api_secret = binance_config.api_secret)
    in_position=False
    closes=[]

    rsi_period=10
    rsi_overbought=70
    rsi_oversold=30
    trade_symbol='ETHUSD'
    trade_quantity=0.05
    plt.rcParams['animation.html']='jshtml'
    fig=plt.figure()
    fig.add_subplot(111)
    plt.show()

    def order(side,quantity, symbol, order_type=ORDER_TYPE_MARKET):
        try:
            print('sending order...')
            order=client.create_order(side=side,quantity=quantity,symbol=symbol,order_type=order_type)
            print(order)

        except Exception as e:
            print (e)
            return False
        return True
    def on_message(wsss,message):
        nonlocal in_position
        json_message=json.loads(message)
        #pprint.pprint( json_message)
        #print ( "received message" , message )
        candle=json_message['k']
        is_candle_closed=candle['x']
        close=candle['c']
        if is_candle_closed==True:
            print("candle_closed_at %s" %candle['c'])
            closes.append(float(close))
            pprint.pprint(closes)
            if len(closes) > rsi_period:
                np_closes=np.array(closes)
                rsi=talib.RSI(np_closes,rsi_period)
                last_rsi=rsi[-1]
                print("the last rsi is {}".format(last_rsi))
                if last_rsi < rsi_oversold:
                    if in_position:
                        print("we already own . Nothing to buy it with")
                    else:
                        print("buy buy buy")
                        # put binance order logic here
                        order_succeeded=order(SIDE_BUY,trade_quantity,trade_symbol)
                        if order_succeeded:
                            in_position=True
                if last_rsi > rsi_overbought:
                    if in_position:
                        print("sell sell sell")
                        #put binance order logic here
                        order_succeeded=order(SIDE_SELL,trade_quantity,trade_symbol)
                        if order_succeeded:
                            in_position=False
                    else:
                        print("we do not have any. Nothing to sell")


        else:
            pass
            #print("candle_is_not_closed_yet")
    def on_open(wsss):
        print("connection established")
    def on_close(wsss):
        print("connection closed")
    wx=websocket.WebSocketApp(socket_var,on_open=on_open,on_close=on_close,on_message=on_message)
    wx.run_forever()

if __name__ == '__main__':
    binance_trading_bot(socket_var)

