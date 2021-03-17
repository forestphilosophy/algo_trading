from pandas.io.html import read_html
from selenium import webdriver

driver = webdriver.Chrome()
driver.get('https://www.tradingview.com/screener/')

table = driver.find_elements_by_class_name('tv-screener-toolbar__button tv-screener-toolbar__button--options tv-screener-toolbar__button--filters apply-common-tooltip common-tooltip-fixed')
#table_html = table.get_attribute('innerHTML')
#table.find_element_by_class_name('tv-screener-toolbar tv-screener-toolbar--standalone  tv-screener-toolbar--standalone_sticky')
#df = read_html(table_html)[0]
#print(df)

#driver.close()
