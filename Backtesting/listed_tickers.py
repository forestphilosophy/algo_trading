file_path = "C:/Users\jimmlin\OneDrive - Deloitte (O365D)\Desktop\Extra/nasdaqlisted.txt"
with open(file_path) as f:
    lines = f.readlines()
lines = lines[1:-1]
tickers = []
for line in range(len(lines)):
    ticker = lines[line].split('|')[0]
    tickers.append(ticker)
print(tickers)
