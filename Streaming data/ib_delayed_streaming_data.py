# -*- coding: utf-8 -*-

# Import libraries
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import sqlite3
import datetime as dt
import pickle
from  datetime import datetime, date
import os.path
import time

class TradeApp(EWrapper, EClient): 
    def __init__(self): 
        EClient.__init__(self, self) 

    def tickString(self, reqId, tickType, value):
        super().tickString(reqId, tickType, value)
        """values = value.split(";")
        if tickType==48:
            print("TickString. TickerId:", reqId, "Type:", tickType,
                  "Last Price:", values[0], "Last Size:", values[1],
                  "Time:", values[2], "VWAP:", values[-2])"""
        
        
        if tickType == 48:
            values = value.split(";")
            values[2] = int(values[2])
            for ms in range(100):
                try:
                    c=db.cursor()
                    print(" TickString. TickerId:", reqId, "Type:", tickType, "Last Price:", values[0], "Last Size:", values[1], "Time:", (dt.datetime.fromtimestamp(int(values[2])/1000)+dt.timedelta(milliseconds=ms)).strftime("%Y%m%d %H:%M:%S.%f"), "VWAP:", values[-2])
                    vals = [(dt.datetime.fromtimestamp(int(values[2])/1000)+dt.timedelta(milliseconds=ms)).strftime("%Y%m%d %H:%M:%S.%f"), values[0], values[1]]
                    query = "INSERT INTO TICKER{}(time,price,volume) VALUES (?,?,?)".format(reqId)
                    c.execute(query,vals)
                    break
                except Exception as e:
                        print(e)
        try:
            db.commit()
        except:
            db.rollback()
        
def usTechStk(symbol,sec_type="STK",currency="USD",exchange="ISLAND"):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    return contract 

def streamSnapshotData(req_num,contract):
    """stream tick leve data"""
    app.reqMktData(reqId=req_num, 
                   contract=contract,
                   genericTickList="233",
                   snapshot=False,
                   regulatorySnapshot=False,
                   mktDataOptions=[])
    
def websocket_con(tickers):
    global db
    db_date = int(date.today().strftime("%Y%m%d"))
    db = sqlite3.connect(f'/Users/jimmylinzhe/Desktop/Algo Trading/Tick data/live_data_{db_date}.db')
    c=db.cursor()
    for i in range(len(tickers)):
        c.execute("CREATE TABLE IF NOT EXISTS TICKER{} (time datetime primary key,price real(15,5), volume integer)".format(i))
    try:
        db.commit()
    except:
        db.rollback()
    app.run()
    
db_date = int(date.today().strftime("%Y%m%d"))    
file_path = f'/Users/jimmylinzhe/Desktop/Algo Trading/pre_order_analysis/evaluation_outputs/outputs_{db_date}.json'
while not os.path.exists(file_path):
    time.sleep(1)
    print ('still running')

if os.path.isfile(file_path):
    with open(file_path, 'rb') as handle:
        stocks_to_order = pickle.load(handle)
    tickers = list(stocks_to_order.keys())
else:
    raise ValueError("something is wrong with the file!")
    

app = TradeApp()
app.connect(host='127.0.0.1', port=7497, clientId=23) #port 4002 for ib gateway paper trading/7497 for TWS paper trading
con_thread = threading.Thread(target=websocket_con,args=(tickers,), daemon=True)
con_thread.start()

run_once = False
while not run_once:
    for ticker in tickers:
        streamSnapshotData(tickers.index(ticker),usTechStk(ticker))
    run_once = True
