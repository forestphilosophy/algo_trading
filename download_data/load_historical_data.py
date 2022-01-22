#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Import libraries
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import pandas as pd
import threading
import time
from os import listdir
from os.path import isfile, join


class TradeApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = {}
        self.reception_tracker = {}
        self.error_msg = {}

    def historicalData(self, reqId, bar):
        if reqId not in self.data:
            self.data[reqId] = [
                {"Date": bar.date, "Open": bar.open, "High": bar.high, "Low": bar.low, "Close": bar.close,
                 "Volume": bar.volume}]
        else:
            self.data[reqId].append(
                {"Date": bar.date, "Open": bar.open, "High": bar.high, "Low": bar.low, "Close": bar.close,
                 "Volume": bar.volume})
        print("reqID:{}, date:{}, open:{}, high:{}, low:{}, close:{}, volume:{}".format(reqId, bar.date, bar.open,
                                                                                        bar.high, bar.low, bar.close,
                                                                                        bar.volume))

    # include this callback to track progress and maybe disconnectwhen all are finished
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        global tickers
        print(f"Finished {reqId}.")
        self.reception_tracker[reqId] = True

    def error(self, reqId: int, errorCode: int, errorString: str):
        super().error(reqId, errorCode, errorString)
        print("Error. Id:", reqId, "Code:", errorCode, "Msg:", errorString)
        self.error_msg[reqId] = errorCode


def stocks(symbol, sec_type="STK", currency="USD", exchange="SMART"):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    return contract


def histData(req_num, contract, duration, candle_size):
    """extracts historical data"""
    app.reqHistoricalData(reqId=req_num,
                          contract=contract,
                          endDateTime='',
                          durationStr=duration,
                          barSizeSetting=candle_size,
                          whatToShow='ADJUSTED_LAST',
                          useRTH=1,
                          formatDate=1,
                          keepUpToDate=0,
                          chartOptions=[])  # EClient function to request contract details


def websocket_con():
    app.run()


###################app connection#######################
app = TradeApp()
app.connect(host='127.0.0.1', port=7497,
            clientId=23)  # port 4002 for ib gateway paper trading/7497 for TWS paper trading
con_thread = threading.Thread(target=websocket_con, daemon=True)
con_thread.start()
time.sleep(1)  # some latency added to ensure that the connection is established

###################storing trade app object in dataframe#######################

tickers = pd.read_csv('/Users/jimmy/Downloads/nasdaq_screener_1639391578277.csv')['Symbol'].tolist()

mypath = 'C:/Users/jimmy/OneDrive/Desktop/Algo Trading/backtesting/data/'

files = [f for f in listdir(mypath) if isfile(join(mypath, f))]
loaded_tickers = [i.split('.')[0] for i in files]
not_loaded_tickers = list(set(tickers) - set(loaded_tickers))

tickers = not_loaded_tickers

for ticker in tickers:
    print(f"pulling data for: {ticker}")
    reqId = tickers.index(ticker)
    histData(reqId, stocks(ticker), '2 Y', '1 hour')

    while reqId not in app.reception_tracker.keys() and reqId not in app.error_msg.keys():  # if the reqId has not been created => wait
        time.sleep(1)

    if reqId in app.error_msg.keys():
        if app.error_msg[reqId] in [162,
                                    200]:  # error handling for there's no market subscription for certain tickers or unclear contract description
            print("I caught this hoe that don't wanna load!")
            continue

    while not app.reception_tracker[reqId]:  # if not data has not been fully pulled => wait
        time.sleep(1)

    df = pd.DataFrame(app.data[reqId])
    df.set_index("Date", inplace=True)
    df.to_csv(f"C:/Users/jimmy/OneDrive/Desktop/Algo Trading/backtesting/data/{ticker}.csv")
