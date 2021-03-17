# -*- coding: utf-8 -*-
"""
IB API - Overnight holding strategy

@author: Jimmy Z. Lin 
"""

# Import libraries
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.execution import ExecutionFilter
from ibapi.order import Order
import pandas as pd
import threading
import time
import statistics
from datetime import datetime, timedelta, date
import numpy as np
from pandas_datareader import data as pdr


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
        self.cash_df = pd.DataFrame()

    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        print("NextValidId:", orderId)

    def updateAccountValue(self, key, val, currency,
                           accountName):
        super().updateAccountValue(key, val, currency, accountName)
        dictionary = {"Key": key, "Value": val, "Currency": currency,
                      "AccountName": accountName}
        self.cash_df = self.cash_df.append(dictionary, ignore_index=True)

    def accountSummary(self, reqId, account, tag, value, currency):
        super().accountSummary(reqId, account, tag, value, currency)
        dictionary = {"ReqId": reqId, "Account": account, "Tag": tag, "Value": value, "Currency": currency}
        self.acc_summary = self.acc_summary.append(dictionary, ignore_index=True)

    def position(self, account, contract, position, avgCost):
        super().position(account, contract, position, avgCost)
        dictionary = {"Account": account, "Symbol": contract.symbol, "SecType": contract.secType,
                      "Currency": contract.currency, "Position": position, "Avg cost": avgCost}
        if contract.symbol in self.pos_df.Symbol.values:
            pass
        else:
            self.pos_df = self.pos_df.append(dictionary, ignore_index=True)

    def historicalData(self, reqId, bar):
        if reqId not in self.data:
            self.data[reqId] = [
                {"Date": bar.date, "Open": bar.open, "High": bar.high, "Low": bar.low, "Close": bar.close,
                 "Volume": bar.volume}]
            print(f"Data ingested for {stock_ticker} for {bar.date}")

        else:
            self.data[reqId].append(
                {"Date": bar.date, "Open": bar.open, "High": bar.high, "Low": bar.low, "Close": bar.close,
                 "Volume": bar.volume})
            print(f"Data ingested for {stock_ticker} for {bar.date}")

    def openOrder(self, orderId, contract, order, orderState):
        super().openOrder(orderId, contract, order, orderState)
        dictionary = {"PermId": order.permId, "ClientId": order.clientId, "OrderId": orderId,
                      "Account": order.account, "Symbol": contract.symbol, "SecType": contract.secType,
                      "Exchange": contract.exchange, "Action": order.action, "OrderType": order.orderType,
                      "TotalQty": order.totalQuantity, "CashQty": order.cashQty,
                      "LmtPrice": order.lmtPrice, "AuxPrice": order.auxPrice, "Status": orderState.status}
        self.order_df = self.order_df.append(dictionary, ignore_index=True)

    def execDetails(self, reqId, contract, execution):
        super().execDetails(reqId, contract, execution)
        print("ExecDetails. ReqId:", reqId, "Symbol:", contract.symbol, "SecType:", contract.secType, "Currency:",
              contract.currency, execution)


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


def histData_15min(req_num, contract, duration, candle_size):
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


def MACD(DF, a=12, b=26, c=9):
    """function to calculate MACD
       typical values a(fast moving average) = 12; 
                      b(slow moving average) =26; 
                      c(signal line ma window) =9"""
    df = DF.copy()
    df["MA_Fast"] = df["Close"].ewm(span=a, min_periods=a).mean()
    df["MA_Slow"] = df["Close"].ewm(span=b, min_periods=b).mean()
    df["MACD"] = df["MA_Fast"] - df["MA_Slow"]
    df["Signal"] = df["MACD"].ewm(span=c, min_periods=c).mean()
    return df


def stochastics(DF, k=14, d=3):
    """
    Fast stochastic calculation
    %K = (Current Close - Lowest Low)/
    (Highest High - Lowest Low) * 100
    %D = 3-day SMA of %K

    Slow stochastic calculation
    %K = %D of fast stochastic
    %D = 3-day SMA of %K

    When %K crosses above %D, buy signal 
    When the %K crosses below %D, sell signal
    """

    df = DF.copy()
    # Set minimum low and maximum high of the k stoch
    low_min = df['low'].rolling(window=k).min()
    high_max = df['high'].rolling(window=k).max()
    # Fast Stochastic
    df['%k_fast'] = 100 * (df['close'] - low_min) / (high_max - low_min)
    df['%d_fast'] = df['%k_fast'].rolling(window=d).mean()
    # Slow Stochastic
    df['%k_slow'] = df["%d_fast"]
    df['%d_slow'] = df['%k_slow'].rolling(window=d).mean()
    return df


def avg_volume(DF, d=30):
    df = DF.copy()
    df['avg_volume'] = df['Volume'].rolling(window=d).mean()
    return df


def atr(DF, n=9):
    "function to calculate True Range and Average True Range"
    df = DF.copy()
    df['H-L'] = abs(df['High'] - df['Low'])
    df['H-PC'] = abs(df['High'] - df['Close'].shift(1))
    df['L-PC'] = abs(df['Low'] - df['Close'].shift(1))
    df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1, skipna=False)
    # df['ATR'] = df['TR'].rolling(n).mean()
    df['ATR'] = df['TR'].ewm(com=n, min_periods=n).mean()
    return df['ATR']


def marketOrder(direction, quantity):
    order = Order()
    order.action = direction
    order.orderType = "MKT"
    order.totalQuantity = quantity
    return order


def stopOrder(direction, quantity, st_price):
    order = Order()
    order.action = direction
    order.orderType = "STP"
    order.totalQuantity = quantity
    order.auxPrice = st_price
    return order


def limitOrder(direction, quantity, LimitPrice):
    order = Order()
    order.action = direction
    order.orderType = "LMT"
    order.totalQuantity = quantity
    order.lmtPrice = LimitPrice
    return order


def StopLimitOrder(direction, quantity, limitPrice, stopPrice):
    order = Order()
    order.action = direction
    order.orderType = "STP LMT"
    order.totalQuantity = quantity
    order.lmtPrice = limitPrice
    order.auxPrice = stopPrice
    return order


def TrailingStopOrder(direction, quantity, trailingPercent, trailStopPrice=False):
    order = Order()
    order.action = direction
    order.orderType = "TRAIL"
    order.totalQuantity = quantity
    order.trailingPercent = trailingPercent
    order.trailStopPrice = trailStopPrice
    return order


def websocket_con():
    app.run()


tickers = ['INFN', 'IMGN', 'GIII', 'CNDT', 'GOGO', 'PTEN', 'FOSL', 'HCAC', 'GMDA', 'SYRS', 'HMHC', 'AXTI', 'GNUS', 'BDSI', 'CRIS', 'LXRX', 'CMRX', 'SESN', 'INFI', 'POWW', 'ARPO', 'CTXR', 'SINO', 'NURO']
app = TradeApp()
app.connect(host='127.0.0.1', port=7497,
            clientId=100)  # port 4002 for ib gateway paper trading/7497 for TWS paper trading
con_thread = threading.Thread(target=websocket_con, daemon=True)
con_thread.start()


###################storing trade app object in dataframe#######################
def dataDataframe(TradeApp_obj, symbols, symbol):
    "returns extracted historical data in dataframe format"
    df = pd.DataFrame(TradeApp_obj.data[symbols.index(symbol)])
    df.set_index("Date", inplace=True)
    df_data[symbol] = df
    return df_data


def dataDataframe_15min(TradeApp_obj, symbols, symbol):
    "returns extracted historical data in dataframe format"
    df = pd.DataFrame(TradeApp_obj.data[symbols.index(symbol) + index])
    df.set_index("Date", inplace=True)
    df.index = pd.to_datetime(df.index)
    stocks_to_order[symbol] = df
    return stocks_to_order


def get_hist_5min(symbols, ticker, db):
    token = symbols.index(ticker)
    data = pd.read_sql('''SELECT * FROM TICKER%s WHERE time >=  date() - '1 day';''' % token, con=db)
    data = data.set_index(['time'])
    data.index = pd.to_datetime(data.index)
    try:
        data['price'].replace('', np.nan, inplace=True)
        data.dropna(inplace=True)
        data['price'] = data['price'].astype('float64')
        price_ohlc = data.loc[:, ['price']].resample('5min').ohlc().dropna()
        price_ohlc.columns = ['open', 'high', 'low', 'close']
        vol_ohlc = data.loc[:, ['volume']].resample('5min').apply({'volume': 'sum'}).dropna()
        df = price_ohlc.merge(vol_ohlc, left_index=True, right_index=True)
        return df
    except:
        print("something went wrong")


###################main script#######################
# extract and store historical data in dataframe repetitively
app.reqAccountSummary(1, "All", "$LEDGER:ALL")
time.sleep(1)
app.reqAccountUpdates(True, 'DU2104193')
time.sleep(1)
cash_df = app.cash_df
cash_amount = float(cash_df.loc[cash_df['Key'] == 'FullAvailableFunds'].Value.values[0])
day_trades_left = int(cash_df.loc[cash_df['Key'] == 'DayTradesRemaining'].Value.values[0])
# traded_prices = app.reqExecutions(reqid, ExecutionFilter())

evaluated_today = False
bought_today = False
sold_today = False
finished_today = False

while True:
    if datetime.now().hour == 21 and datetime.now().minute == 57 and evaluated_today is False:
        df_data = {}
        stocks_to_order = {}
        for ticker in tickers:
            try:
                start_date = datetime.now() - timedelta(days=90)
                end_date = date.today()
                df = pdr.get_data_yahoo(ticker, start=start_date, end=end_date)
                df['avg_volume'] = avg_volume(df)['avg_volume']
                resistance_level = df['High'][:-1].max()
                df_data[ticker] = df
                print (f'Data request for {ticker}.')
                if (df['Volume'][-1] > df['avg_volume'][-1]) and \
                        (df['avg_volume'][-1] > 500000) and \
                        (df['Volume'][-1] > 500000) and \
                        (df['Close'][-1] > resistance_level):
                    quantity = int(cash_amount * 0.02 / df_data[ticker].Close[-1])
                    stocks_to_order[ticker] = quantity

            except Exception as e:
                print(e)

        evaluated_today = True

    elif datetime.now().hour == 21 and datetime.now().minute == 58 and bought_today is False:
        for ticker, quantity in stocks_to_order.items():
            app.reqIds(-1)
            time.sleep(1)
            order_id = app.nextValidOrderId
            print(f'Buy {quantity} {ticker}')
            app.placeOrder(order_id, stocks(ticker), marketOrder("BUY", quantity))
        bought_today = True

    elif datetime.now().hour == 23 and datetime.now().minute == 40 and finished_today is False:
        for ticker, quantity in stocks_to_order.items():
            df_data[ticker]["atr"] = atr(df_data[ticker])
            app.reqIds(-1)
            time.sleep(1)
            order_id = app.nextValidOrderId
            app.placeOrder(order_id, stocks(ticker),
                           limitOrder("SELL", quantity, round(df_data[ticker]["Close"][-1] +
                                                              df_data[ticker]["atr"][-1], 1)))
            app.placeOrder(order_id + 1, stocks(ticker), stopOrder("SELL", quantity,
                                                                   round(df_data[ticker]["Close"][-1] - 0.5 *
                                                                         df_data[ticker]["atr"][-1], 1)))
        finished_today = True

    elif datetime.now().hour == 15 and datetime.now().minute == 30 and sold_today is False:
        # placing orders 1 min after market open
        app.reqGlobalCancel()
        app.reqIds(-1)
        time.sleep(2)
        order_id = app.nextValidOrderId
        app.reqPositions()
        time.sleep(2)
        pos_df = app.pos_df[app.pos_df.Position != 0]
        pos_df.drop_duplicates(inplace=True)
        for ticker in pos_df['Symbol']:
            quantity = pos_df[pos_df["Symbol"] == ticker]["Position"].values[0]
            if quantity > 0:
                app.placeOrder(order_id, stocks(ticker), marketOrder("SELL", quantity))
                order_id += 1
            elif quantity < 0:
                app.placeOrder(order_id, stocks(ticker), marketOrder("BUY", -quantity))
                order_id += 1
            else:
                continue
        sold_today = True

    else:
        continue
