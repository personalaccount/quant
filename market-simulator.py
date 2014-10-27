import pandas as pd
import numpy as np
import math
import copy
import QSTK.qstkutil.qsdateutil as du
import datetime as dt
import QSTK.qstkutil.DataAccess as da
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkstudy.EventProfiler as ep

# Read-in the orders
orders_csv_filename = "orders-short.csv"
#orders_csv_filename = "orders2.csv"
cash_amount = 1000000

# Create a numpy array of orders
na_orders_list = np.loadtxt(orders_csv_filename, dtype={'names':('year', 'month', 'day', 'symbol', 'order_type', 'num_shares'), 
    'formats':('i4','i2','i2','S4','S4','i4')}, delimiter=',',skiprows=0)

num_of_orders = len(na_orders_list)
last_row = num_of_orders-1

# A list of symbols
lSymbols = []

# A list of dates when orders are occured
lDates = []

for i in range (num_of_orders):
    # Fill-in the list of Dates
    order_date = dt.datetime(na_orders_list[i]['year'],na_orders_list[i]['month'],na_orders_list[i]['day']) + dt.timedelta(hours=16)
    # Since there may be multiple orders on the same date it is important to check for duplicates
    if (order_date not in lDates):
        lDates.append(order_date)

    # Fill-in the list of Symbols
    if (na_orders_list[i]['symbol'] not in lSymbols):
        lSymbols.append(na_orders_list[i]['symbol'])

# Set the start date
startDate = dt.datetime(na_orders_list[0]['year'], na_orders_list[0]['month'], na_orders_list[0]['day'])

# End date should be offset-ed by 1 day in order to read the adjusted_close for the last date. 
endDate = dt.datetime(na_orders_list[last_row]['year'], na_orders_list[last_row]['month'], na_orders_list[last_row]['day'])+dt.timedelta(days=1)

# Specify 16:00 hours to read the data that was available at the close of the trading day
dt_timeofday = dt.timedelta(hours=16)

# The getNYSEdays() function tells if the market was open on a particular day
# Get a list of timestamps that represent NYSE closing times between the start and end dates
ldt_timestamps = du.getNYSEdays(startDate, endDate, dt_timeofday)

# Number of trading days in a period
num_of_tradedays = len(ldt_timestamps)

# Create an empty pandas dataframe for the portfolio
# and fill it with 0
df_Portfolio = pd.DataFrame(index=ldt_timestamps, columns=lSymbols)
df_Portfolio = df_Portfolio.fillna(0)

print "Start date: ", startDate
print "End date: ", endDate
print "Trade days: ", num_of_tradedays
print "Initial capital: $", cash_amount

# Create an object that will be ready to read from Yahoo data source
c_dataobj = da.DataAccess('Yahoo', cachestalltime=0)

# Create a list of dataframe objects which have all the different types of data
ldf_data = c_dataobj.get_data(ldt_timestamps, lSymbols, 'close')

# Add an extra column "CASH" to the portfolio dataframe. Defaults to the total amount of cash available in the beginning
df_Portfolio["CASH"] = df_Portfolio[lSymbols[0]].map(lambda x:cash_amount)

# Add an extra column "P_VALUE" to the portfolio dataframe, to represent portfolio value. Defaults to the starting amount of cash
df_Portfolio["P_VALUE"] = df_Portfolio[lSymbols[0]].map(lambda x:cash_amount)

# Now we have the orders, sorted by date and we have the equity prices at each given date (ldf_data)
# Iterate over the orders, check the prices and update the portfolio

for j in range(0,num_of_orders):
    order_date  = dt.datetime(na_orders_list[j]['year'],na_orders_list[j]['month'],na_orders_list[j]['day']) + dt.timedelta(hours=16)
    symbol      = na_orders_list[j]['symbol']
    num_shares  = na_orders_list[j]['num_shares']
    order_type  = na_orders_list[j]['order_type']

    # The .loc attribute is the primary access method to the cells in a pandas dataframe
    share_price = ldf_data.loc[order_date,symbol]
    order_value = num_shares*share_price

    print "\r\nOrder date: %s" % order_date
    print "Processing: ",order_type," ",num_shares," shares of ",symbol
    print symbol," share market price is $%s" % share_price
    print "Total order amount: $%s\r\n" % order_value

    current_num_shares = df_Portfolio.loc[order_date,symbol]

    if order_type == "Buy":
        cash_amount = cash_amount - order_value
        new_num_shares = current_num_shares + num_shares

    if order_type == "Sell":
        cash_amount = cash_amount + order_value
        new_num_shares = current_num_shares - num_shares
    
    df_Portfolio.loc[order_date:,symbol] = new_num_shares
    df_Portfolio.loc[order_date:,['CASH']] = cash_amount

    cur_portfolio_value = cash_amount


    for sym in lSymbols:
        cur_portfolio_value += df_Portfolio.loc[order_date,sym]*ldf_data.loc[order_date,sym]
        print "%4s value on %s is $%6s |%d shares" % (sym, order_date, ldf_data.loc[order_date, sym], df_Portfolio.loc[order_date,sym])
    
    df_Portfolio.loc[order_date:,["P_VALUE"]] = cur_portfolio_value


for market_date in ldt_timestamps:
    cur_portfolio_value = float(df_Portfolio.loc[market_date,['CASH']])
    for sym in lSymbols:
        cur_portfolio_value += df_Portfolio.loc[market_date,sym]*ldf_data.loc[market_date,sym]
    
    df_Portfolio.loc[market_date,["P_VALUE"]] = cur_portfolio_value


print "\r\n"
print df_Portfolio

print "\r\nDetails on performance of the portfolio:"
print "Data Range: ", startDate, "to", endDate,"\r\n"

# Calculating the statistics
portfolio_cummulative_rets = np.array(df_Portfolio.loc[:,"P_VALUE"])
normalized_portfolio_rets = portfolio_cummulative_rets[:]/portfolio_cummulative_rets[0]

port_dreturns = tsu.returnize0(normalized_portfolio_rets)

# Standard Deviation of Fund
fDev = np.std(port_dreturns)
fMean = np.mean(port_dreturns)
fSharpe = (fMean*252)/(fDev*np.sqrt(252))

print "Sharpe Ratio of Fund :",fSharpe
print "Total Return of Fund :", portfolio_cummulative_rets[-1]/portfolio_cummulative_rets[0]
print "Standard Deviation of Fund :", fDev
print "Average Daily Return of Fund :", fMean