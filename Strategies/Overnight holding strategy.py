# -*- coding: utf-8 -*-
"""
IB API - Overnight holding strategy

@author: Jimmy Z. Lin 
"""

# Import libraries
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import pandas as pd
import threading
import time
import statistics
import datetime


class TradeApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = {}
        self.acc_summary = pd.DataFrame(columns=['ReqId', 'Account', 'Tag', 'Value', 'Currency'])
        self.pos_df = pd.DataFrame(columns=['Account', 'Symbol', 'SecType',
                                            'Currency', 'Position', 'Avg cost'])
        self.order_df = pd.DataFrame(columns=['PermId', 'ClientId', 'OrderId',
                                              'Account', 'Symbol', 'SecType',
                                              'Exchange', 'Action', 'OrderType',
                                              'TotalQty', 'CashQty', 'LmtPrice',
                                              'AuxPrice', 'Status'])

    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        print("NextValidId:", orderId)

    def accountSummary(self, reqId, account, tag, value, currency):
        super().accountSummary(reqId, account, tag, value, currency)
        dictionary = {"ReqId": reqId, "Account": account, "Tag": tag, "Value": value, "Currency": currency}
        self.acc_summary = self.acc_summary.append(dictionary, ignore_index=True)

    def position(self, account, contract, position, avgCost):
        super().position(account, contract, position, avgCost)
        dictionary = {"Account": account, "Symbol": contract.symbol, "SecType": contract.secType,
                      "Currency": contract.currency, "Position": position, "Avg cost": avgCost}
        self.pos_df = self.pos_df.append(dictionary, ignore_index=True)

    def historicalData(self, reqId, bar):
        print(f'Time: {bar.date}, Open: {bar.open}, Close: {bar.close}')
        if reqId not in self.data:
            self.data[reqId] = [
                {"Date": bar.date, "Open": bar.open, "High": bar.high, "Low": bar.low, "Close": bar.close,
                 "Volume": bar.volume}]
            print(f'Time: {bar.date}, Open: {bar.open}, Close: {bar.close}')

        else:
            if len(self.data[reqId]) <= 4:  # make sure no repetitive data is requested
                self.data[reqId].append(
                    {"Date": bar.date, "Open": bar.open, "High": bar.high, "Low": bar.low, "Close": bar.close,
                     "Volume": bar.volume})
                print(f'Time: {bar.date}, Open: {bar.open}, Close: {bar.close}')
            else:
                pass

    def openOrder(self, orderId, contract, order, orderState):
        super().openOrder(orderId, contract, order, orderState)
        dictionary = {"PermId": order.permId, "ClientId": order.clientId, "OrderId": orderId,
                      "Account": order.account, "Symbol": contract.symbol, "SecType": contract.secType,
                      "Exchange": contract.exchange, "Action": order.action, "OrderType": order.orderType,
                      "TotalQty": order.totalQuantity, "CashQty": order.cashQty,
                      "LmtPrice": order.lmtPrice, "AuxPrice": order.auxPrice, "Status": orderState.status}
        self.order_df = self.order_df.append(dictionary, ignore_index=True)


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


def marketOrder(direction, quantity):
    order = Order()
    order.action = direction
    order.orderType = "MKT"
    order.totalQuantity = quantity
    return order


def websocket_con():
    app.run()


app = TradeApp()
app.connect(host='127.0.0.1', port=7497,
            clientId=23)  # port 4002 for ib gateway paper trading/7497 for TWS paper trading
con_thread = threading.Thread(target=websocket_con, daemon=True)
con_thread.start()


###################storing trade app object in dataframe#######################
def dataDataframe(TradeApp_obj, symbols):
    "returns extracted historical data in dataframe format"
    df_data = {}
    for symbol in symbols:
        try:
            df_data[symbol] = pd.DataFrame(TradeApp_obj.data[symbols.index(symbol)])
            df_data[symbol].set_index("Date", inplace=True)
        except:
            continue
    return df_data


# extract and store historical data in dataframe repetitively
tickers = ['HWM',
           'AES',
           'APA']
app.reqAccountSummary(1, "All", "$LEDGER:ALL")
time.sleep(1)
acc_summ_df = app.acc_summary
cash_balance = float(
    acc_summ_df.loc[(acc_summ_df['Tag'] == 'CashBalance') & (acc_summ_df['Currency'] == 'BASE')]['Value'].item())

for ticker in tickers:
    histData(tickers.index(ticker), stocks(ticker), '5 D', '1 day')
    time.sleep(4)
historicalData = dataDataframe(app, tickers)

list_of_prices = []
list_of_ratings = []
list_of_symbols = []

for key, df in historicalData.items():
    df['price_chg'] = df['Close'][-1] - df['Close'][0]
    df['volume_stdev'] = statistics.stdev(df['Volume'][:-1])
    df['volume_chg'] = df['Volume'][-1] - df['Volume'][-2]
    df['volume_factor'] = df['volume_chg'] / df['volume_stdev']
    df['rating'] = df['price_chg'] / df['Close'][0] * df['volume_factor']
    list_of_prices.append(df['Close'][-1])
    list_of_ratings.append(df['rating'][-1])
    list_of_symbols.append(key)

zipped_list = list(zip(list_of_symbols, list_of_ratings, list_of_prices))
ratings = pd.DataFrame(zipped_list, columns=['symbol', 'rating', 'price'])
ratings = ratings[ratings['rating'] > 0].sort_values('rating', ascending=False)[:10]

# how many shares to buy
total_rating = ratings['rating'].sum()
shares = {}
for _, row in ratings.iterrows():
    shares[row['symbol']] = int(row['rating'] / total_rating * cash_balance / row['price'])

order_id = app.nextValidOrderId
while True:
    if datetime.datetime.now().hour == 21 and datetime.datetime.now().minute == 58:  # placing orders 2 mins before market close
        for ticker in ratings['symbol']:
            quantity = shares[ticker]
            app.placeOrder(order_id, stocks(ticker), marketOrder("BUY", quantity))
            order_id += 1
        break

    if datetime.datetime.now().hour == 15 and datetime.datetime.now().minute == 31:  # closing all positions next morning 2 mins after market open
        app.reqGlobalCancel()
        app.reqIds(-1)
        time.sleep(1)
        order_id = app.nextValidOrderId
        app.reqPositions()
        time.sleep(2)
        pos_df = app.pos_df
        pos_df.drop_duplicates(inplace=True, ignore_index=True)

        for ticker in pos_df['Symbol']:
            quantity = pos_df[pos_df["Symbol"] == ticker]["Position"].values[0]
            app.placeOrder(order_id, stocks(ticker), marketOrder("SELL", quantity))
            order_id += 1
        break
