# -*- coding: utf-8 -*-
"""
IB API - accessing data in the sql db

@author: Mayank Rasu (http://rasuquant.com/wp/)
"""

import sqlite3
import pandas as pd
from datetime import datetime,timedelta
import statistics
import json

def get_hist_1min(ticker,db):
    token = tickers.index(ticker)
    data = pd.read_sql('''SELECT * FROM TICKER%s WHERE time >=  date() - '1 day';''' %token, con=db)                
    data = data.set_index(['time'])
    data.index = pd.to_datetime(data.index)
    price_ohlc= data.loc[:, ['price']].resample('1min').ohlc().dropna()
    price_ohlc.columns = ['open','high','low','close']
    vol_ohlc = data.loc[:, ['volume']].resample('1min').apply({'volume': 'sum'}).dropna()
    df = price_ohlc.merge(vol_ohlc,left_index=True, right_index=True)
    return df

def get_hist_last2mins(ticker,db):
    token = tickers.index(ticker)
    data = pd.read_sql('''SELECT * FROM TICKER%s WHERE time >=  date() - '2 mins';''' %token, con=db)                
    data = data.set_index(['time'])
    data.index = pd.to_datetime(data.index)
    price_ohlc= data.loc[:, ['price']].resample('1min').ohlc().dropna()
    price_ohlc.columns = ['open','high','low','close']
    vol_ohlc = data.loc[:, ['volume']].resample('1min').apply({'volume': 'sum'}).dropna()
    df = price_ohlc.merge(vol_ohlc,left_index=True, right_index=True)
    return df
    
def MACD(DF,a=12,b=26,c=9):
    """function to calculate MACD
       typical values a(fast moving average) = 12; 
                      b(slow moving average) =26; 
                      c(signal line ma window) =9"""
    df = DF.copy()
    df["MA_Fast"]=df["Close"].ewm(span=a,min_periods=a).mean()
    df["MA_Slow"]=df["Close"].ewm(span=b,min_periods=b).mean()
    df["MACD"]=df["MA_Fast"]-df["MA_Slow"]
    df["Signal"]=df["MACD"].ewm(span=c,min_periods=c).mean()
    return df

tickers = ["FB","INTC","AMZN"]
db = sqlite3.connect('C:/Users/jimmlin/OneDrive - Deloitte (O365D)/Desktop/Algo Trading/Tick data/Streaming data.db')
c=db.cursor()

#print out names of all the tables in DB
c.execute('SELECT name from sqlite_master where type= "table"')
c.fetchall()

#print out the columns and column types of a given table
c.execute('''PRAGMA table_info(TICKER0)''')
c.fetchall()

#print all rows for a given table
for m in c.execute('''SELECT * FROM TICKER0'''):
    print(m)

##retrieving cash balance from other sciprt
with open(r'C:/Users/jimmlin/OneDrive - Deloitte (O365D)/Desktop/Algo Trading/pre_order_analysis/cash_balance_inputs/cash_balance.json') as f:
            cash_balance = json.load(f)

list_of_prices = []
list_of_ratings = []
list_of_symbols = []
evaluated_today = False
df_data = {}
while True:
    if datetime.now().hour == 21 and datetime.now().minute == 57 and evaluated_today == False:#perform pre order analysis
        for ticker in tickers:
            df_data[ticker] = (get_hist_1min(ticker,db))
            df_data[ticker]['price_chg'] = df_data[ticker]['Close'][-1]-df_data[ticker]['Close'][0]
            df_data[ticker]['volume_stdev'] = statistics.stdev(df_data[ticker]['Volume'][:-1])
            df_data[ticker]['volume_chg'] = df_data[ticker]['Volume'][-1] - df_data[ticker]['Volume'][-2]
            df_data[ticker]['volume_factor'] = df_data[ticker]['volume_chg'] /df_data[ticker]['volume_stdev']
            df_data[ticker]['rating'] = df_data[ticker]['price_chg']/df_data[ticker]['Close'][0]*df_data[ticker]['volume_factor']
            list_of_prices.append(df_data[ticker]['Close'][-1])
            list_of_ratings.append(df_data[ticker]['rating'][-1])
            list_of_symbols.append(ticker)
            
        zipped_list = list(zip(list_of_symbols, list_of_ratings, list_of_prices))
        ratings = pd.DataFrame(zipped_list,columns=['symbol', 'rating', 'price'])
        ratings = ratings[ratings['rating'] > 0].sort_values('rating', ascending=False)
         
        #how many shares to buy 
        total_rating = ratings['rating'].sum()
        shares = {}
        for _, row in ratings.iterrows():
                shares[row['symbol']] = int(row['rating'] / total_rating * cash_balance / row['price'])  

        filtered_dict = {k:v for k,v in df_data.items() if k in ratings['symbol'].tolist()}
        for ticker in filtered_dict.keys():
            filtered_dict[ticker]['shares'] = shares[ticker]
            filtered_dict[ticker]["macd"] = MACD(filtered_dict[ticker])["MACD"]
            filtered_dict[ticker]["signal"] = MACD(filtered_dict[ticker])["Signal"]
            filtered_dict[ticker].dropna(inplace=True)
            filtered_dict[ticker]["diff"] = filtered_dict[ticker]["macd"]-filtered_dict[ticker]["signal"]
            differences = []
            for row in filtered_dict[ticker]['diff']:
                if row > 0:
                    differences.append("Positive")
                else:
                    differences.append("Negative")
            filtered_dict[ticker]["differences"] = differences
            filtered_dict[ticker]['long/short'] = filtered_dict[ticker]['differences'].shift() == filtered_dict[ticker]['differences']    
        with open(r'C:/Users/jimmlin/OneDrive - Deloitte (O365D)/Desktop/Algo Trading/pre_order_analysis/analysis_outputs/output.json', "w") as outfile:  
            json.dump(filtered_dict, outfile) 
        
        if datetime.now().hour == 22 and datetime.now().minute == 1 and evaluated_today == False:#perform pre order analysis
            post_market = {}
            for ticker in ratings['symbol']:
                post_market[ticker]["macd"] = MACD(post_market[ticker])["MACD"]
                post_market[ticker]["signal"] = MACD(post_market[ticker])["Signal"]
                post_market[ticker].dropna(inplace=True)
                post_market[ticker]["diff"] = post_market[ticker]["macd"]-post_market[ticker]["signal"]
                differences = []
                for row in df_data[ticker]['diff']:
                    if row > 0:
                        differences.append("Positive")
                    else:
                        differences.append("Negative")
                post_market[ticker]["differences"] = differences
                post_market[ticker]['long/short'] = post_market[ticker]['differences'].shift() == post_market[ticker]['differences']
                #if post_market[ticker]['long/short'][-1] != 
    evaluated_today = True    