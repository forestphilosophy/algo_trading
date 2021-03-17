file_path = "C:/Users\jimmy\OneDrive\Desktop\Algo Trading\Stock screeners/america_2020-12-08.csv"
with open(file_path) as f:
    lines = f.readlines()
lines = lines[1:]
tickers = []
for line in range(len(lines)):
    ticker = lines[line].split(',')[0]
    tickers.append(ticker)
print(tickers)