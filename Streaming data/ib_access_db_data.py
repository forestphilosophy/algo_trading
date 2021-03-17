
import sqlite3
import pandas as pd
from datetime import datetime,timedelta
import statistics
import numpy as np
def get_hist_1min(ticker,db):
    token = tickers.index(ticker)
    data = pd.read_sql('''SELECT * FROM TICKER%s WHERE time >=  date() - '1 day';''' %token, con=db)                
    data = data.set_index(['time'])
    data.index = pd.to_datetime(data.index)
    try:
        data['price'].replace('', np.nan, inplace=True)
        data.dropna(inplace = True)
        data['price'] = data['price'].astype('float64')
        price_ohlc= data.loc[:, ['price']].resample('1min').ohlc().dropna()
        price_ohlc.columns = ['open','high','low','close']
        vol_ohlc = data.loc[:, ['volume']].resample('1min').apply({'volume': 'sum'}).dropna()
        df = price_ohlc.merge(vol_ohlc,left_index=True, right_index=True)
        return df
    except:
        print ("something went wrong")
        
    
def MACD(DF,a=12,b=26,c=9):
    """function to calculate MACD
       typical values a(fast moving average) = 12; 
                      b(slow moving average) =26; 
                      c(signal line ma window) =9"""
    df = DF.copy()
    df["MA_Fast"]=df["CLOSE"].ewm(span=a,min_periods=a).mean()
    df["MA_Slow"]=df["CLOSE"].ewm(span=b,min_periods=b).mean()
    df["MACD"]=df["MA_Fast"]-df["MA_Slow"]
    df["Signal"]=df["MACD"].ewm(span=c,min_periods=c).mean()
    return df

tickers = ['BAC', 'HBAN', 'MGM', 'OI', 'PCG', 'ANF', 'MDRX', 'GES', 'GPK', 'SBRA', 'EVRI', 'SBH', 'RF', 'CIM', 'PEB', 'STNG', 'GNMK', 'TRGP', 'NLSN', 'ZNGA', 'MTDR', 'BLMN', 'RLGY', 'TPH', 'SEAS', 'NRZ', 'DOC', 'SFM', 'BRX', 'CHRS', 'AXTA', 'IGT', 'WSC', 'RRR', 'USFD', 'VVV', 'MYOV', 'PK', 'DXC', 'CLDR', 'VST', 'MRSN', 'SMPL', 'CARG', 'ZUO', 'CHX', 'EB', 'AMCR', 'CHNG', 'REAL']
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

df_data = {}
for ticker in tickers:
    df_data[ticker] = (get_hist_1min(ticker,db))


list_of_prices = []
list_of_ratings = []
list_of_symbols = []
for ticker in tickers:
    df = get_hist_1min(ticker,db)
    df['price_chg'] = df['CLOSE'].iloc[-1]-df['CLOSE'].iloc[0]
    df['volume_stdev'] = statistics.stdev(df['VOLUME'].iloc[:-1])
    df['volume_chg'] = df['VOLUME'].iloc[-1] - df['VOLUME'].iloc[-2]
    df['volume_factor'] = df['volume_chg'] /df['volume_stdev']
    df['rating'] = df['price_chg']/df['CLOSE'].iloc[0]*df['volume_factor']
    list_of_prices.append(df['CLOSE'].iloc[-1])
    list_of_ratings.append(df['rating'][-1])
    list_of_symbols.append(ticker)
zipped_list = list(zip(list_of_symbols, list_of_ratings, list_of_prices))
ratings = pd.DataFrame(zipped_list,columns=['symbol', 'rating', 'price'])
ratings = ratings[ratings['rating'] > 0].sort_values('rating', ascending=False)
 
#how many shares to buy 
total_rating = ratings['rating'].sum()
shares = {}
for _, row in ratings.iterrows():
        shares[row['symbol']] = int(row['rating'] / total_rating * 5000 / row['price'])  
        
for ticker in ratings['symbol']:   
    df["macd"] = MACD(df)["MACD"]
    df["signal"] = MACD(df)["Signal"]
    df.dropna(inplace=True)
    df["diff"] = df["macd"]-df["signal"]
    differences = []
    for row in df['diff']:
        if row > 0:
            differences.append("Positive")
        else:
            differences.append("Negative")
    df["differences"] = differences
    df['long/short'] = df['differences'].shift() == df['differences']

