from zipline import run_algorithm
from zipline.api import order_target_percent, symbol, schedule_function, date_rules, time_rules
from datetime import datetime
import pytz
import pyfolio as pf
import pandas as pd
import matplotlib
from matplotlib import pyplot as plt, rc, ticker
import numpy as np

matplotlib.use('Agg')


def initialize(context):
    # Which stocks to trade
    dji = [
        "AAPL",
        "AXP",
        "BA",
        "CAT",
        "CSCO",
        "CVX",
        "DIS",
        "DWDP",
        "GS",
        "HD",
        "IBM",
        "INTC",
        "JNJ",
        "JPM",
        "KO",
        "MCD",
        "MMM",
        "MRK",
        "MSFT",
        "NKE",
        "PFE",
        "PG",
        "TRV",
        "UNH",
        "UTX",
        "V",
        "VZ",
        "WBA",
        "WMT",
        "XOM",
    ]

    # Make symbol list from tickers
    context.universe = [symbol(s) for s in dji]

    # History window
    context.history_window = 20

    # Size of our portfolio
    context.stocks_to_hold = 10

    # Schedule the daily trading routine for once per month
    schedule_function(handle_data, date_rules.month_start(), time_rules.market_close())


def month_perf(ts):
    perf = (ts[-1] / ts[0]) - 1
    return perf


def handle_data(context, data):
    # Get history for all the stocks.
    hist = data.history(context.universe, "close", context.history_window, "1d")

    # This creates a table of percent returns, in order.
    perf_table = hist.apply(month_perf).sort_values(ascending=False)

    # Make buy list of the top N stocks
    buy_list = perf_table[:context.stocks_to_hold]

    # The rest will not be held.
    the_rest = perf_table[context.stocks_to_hold:]

    # Place target buy orders for top N stocks.
    for stock, perf in buy_list.iteritems():
        stock_weight = 1 / context.stocks_to_hold

        # Place order
        if data.can_trade(stock):
            order_target_percent(stock, stock_weight)

    # Make sure we are flat the rest.
    for stock, perf in the_rest.iteritems():
        # Place order
        if data.can_trade(stock):
            order_target_percent(stock, 0.0)


def analyze(context, perf):
    # Use PyFolio to generate a performance report
    returns, positions, transactions = pf.utils.extract_rets_pos_txn_from_zipline(perf)
    f = pf.create_returns_tear_sheet(returns, benchmark_rets=None, return_fig=True)
    f.savefig('backtest.png')


def annualized_return(ts):
    return np.power((ts[-1] / ts[0]), (YEAR_LENGTH / len(ts))) - 1


def drawdown(ts):
    return np.min(ts / np.maximum.accumulate(ts)) - 1


# Set start and end date
start = pd.Timestamp('2003-1-1', tz='utc')
end = pd.Timestamp('2017-12-31', tz='utc')

# Fire off the backtest
result = run_algorithm(
    start=start,
    end=end,
    initialize=initialize,
    analyze=analyze,
    capital_base=10000,
    data_frequency='daily',
    bundle='quantopian-quandl'
)
# get portfolio value and positions for a specified day
DAY = '2009-03-17'
portfolio_value = result.loc[DAY, 'portfolio_value']
day_positions = result.loc[DAY, 'positions']
df = pd.DataFrame(columns=['value', 'pnl'])
for pos in day_positions:
    ticker = pos['sid'].symbol
    df.loc[ticker, 'value'] = pos['amount'] * pos['last_sale_price']
    df.loc[ticker, 'pnl'] = df.loc[ticker, 'value'] - (pos['amount'] * pos['cost_basis'])

# add cash position, which would be the total portfolio value minux value of stocks we hold
df.loc['cash', ['value', 'pnl']] = [(portfolio_value - df['value'].sum()), 0]

# pie chart for allocations
fix, ax1 = plt.subplots(figsize=[12, 10])
ax1.pie(abs(df['value']), labels=df.index, shadow=True, startangle=90)
ax1.axis('equal')
ax1.set_title('Allocation on {}'.format(DAY))
plt.savefig('tryshit.png')
# bar chart for open PnL (Open profit and loss per position)
# NOTE: this result is different from book chart.. check data
fig, ax1 = plt.subplots(figsize=[12, 10])
pnl_df = df.drop('cash')
ax1.barh(pnl_df.index, pnl_df['pnl'], align='center', color='green', ecolor='black')
ax1.set_title('Open PnL on {}'.format(DAY))
plt.savefig('tryshit.png')

CALC_WINDOW = 126
YEAR_LENGTH = 252
df = result.copy().filter(items=['portfolio_value', 'gross_leverage'])
rolling_window = result.portfolio_value.rolling(CALC_WINDOW)
# Calculate rolling analytics
df['annualized'] = rolling_window.apply(annualized_return)
df['drawdown'] = rolling_window.apply(drawdown)
df.dropna(inplace=True)

# Make a figure
fig = plt.figure(figsize=(12, 12))

# Make the base lower, just to make the graph easier to read
df['portfolio_value'] /= 100

# First chart
ax = fig.add_subplot(411)
ax.set_title('Strategy Results')
ax.plot(df['portfolio_value'],
        linestyle='-',
        color='black',
        label='Equity Curve', linewidth=3.0)

# Set log scale
ax.set_yscale('log')

# Make the axis look nicer
ax.yaxis.set_ticks(np.arange(df['portfolio_value'].min(), df['portfolio_value'].max(), 500))
ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%0.0f'))

# Add legend and grid
ax.legend()
ax.grid(False)

# Second chart
ax = fig.add_subplot(412)
ax.plot(df['gross_leverage'],
        label='Strategy exposure'.format(CALC_WINDOW),
        linestyle='-',
        color='black',
        linewidth=1.0)

# Make the axis look nicer
ax.yaxis.set_ticks(np.arange(df['gross_leverage'].min(), df['gross_leverage'].max(), 0.02))
ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%0.2f'))

# Add legend and grid
ax.legend()
ax.grid(True)

# Third chart
ax = fig.add_subplot(413)
ax.plot(df['annualized'],
        label='{} days annualized return'.format(CALC_WINDOW),
        linestyle='-',
        color='black',
        linewidth=1.0)

# Make the axis look nicer
ax.yaxis.set_ticks(np.arange(df['annualized'].min(), df['annualized'].max(), 0.5))
ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%0.1f'))

# Add legend and grid
ax.legend()
ax.grid(True)

# Fourth chart
ax = fig.add_subplot(414)
ax.plot(df['drawdown'],
        label='{} days max drawdown'.format(CALC_WINDOW),
        linestyle='-',
        color='black',
        linewidth=1.0)

# Make the axis look nicer
ax.yaxis.set_ticks(np.arange(df['drawdown'].min(), df['drawdown'].max(), 0.1))
ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%0.1f'))

# Add legend and grid
ax.legend()
ax.grid(True)

plt.savefig('tryshit2.png')