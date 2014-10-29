import pandas as pd
import numpy as np
import math
import copy
import QSTK.qstkutil.qsdateutil as du
import datetime as dt
import QSTK.qstkutil.DataAccess as da
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkstudy.EventProfiler as ep
import csv

# Read-in the orders
#orders_csv_filename = "orders-short.csv"
#orders_csv_filename = "orders2.csv"
orders_csv_filename = "events.csv"
cash_amount = 50000

# List of symbols in our portfolio
lSymbols = []

# List of orders
lOrderDetails = []

with open(orders_csv_filename, 'rb') as csvfile:
    orders_reader = csv.reader(csvfile, delimiter=',')
    for row in orders_reader:

        # Fill-in the list of Symbols
        symbol = row[3]
        if (symbol not in lSymbols):
            lSymbols.append(symbol)

        order_date = dt.datetime(int(row[0]),int(row[1]),int(row[2])) + dt.timedelta(hours=16)
        lOrderDetails.append([order_date,symbol,row[4],row[5]])

# Sort the array
lOrderDetails = sorted (lOrderDetails, key=lambda orders: orders[0])

num_of_orders = len(lOrderDetails)

# Set the start and end date
startDate = lOrderDetails[0][0]
endDate = lOrderDetails[-1][0]

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
spx_data = c_dataobj.get_data(ldt_timestamps, ["$SPX"], 'close')

# Add an extra column "CASH" to the portfolio dataframe. Defaults to the total amount of cash available in the beginning
df_Portfolio["CASH"] = df_Portfolio[lSymbols[0]].map(lambda x:cash_amount)

# Add an extra column "P_VALUE" to the portfolio dataframe, to represent portfolio value. Defaults to the starting amount of cash
df_Portfolio["P_VALUE"] = df_Portfolio[lSymbols[0]].map(lambda x:cash_amount)

# Now we have the orders, sorted by date and we have the equity prices at each given date (ldf_data)
# Iterate over the orders, check the prices and update the portfolio

for j in range(0,num_of_orders):
    order_date  = lOrderDetails[j][0]
    symbol      = lOrderDetails[j][1]
    order_type  = lOrderDetails[j][2]
    num_shares  = int(lOrderDetails[j][3])

    # The .loc attribute is the primary access method to the cells in a pandas dataframe
    share_price = ldf_data.loc[order_date,symbol]
    order_value = num_shares*share_price

    current_num_shares = df_Portfolio.loc[order_date,symbol]

    print "\r\nOrder date: %s" % order_date
    print "Processing:",order_type,"",num_shares," shares of ",symbol
    print symbol," share market price is: $%s" % share_price
    print "Total order amount: $%s\r\n" % order_value

    if order_type == "Buy":
        cash_amount = cash_amount - order_value
        new_num_shares = current_num_shares + num_shares

    if order_type == "Sell":
        cash_amount = cash_amount + order_value
        new_num_shares = current_num_shares - num_shares
    
    df_Portfolio.loc[order_date:,symbol] = new_num_shares
    df_Portfolio.loc[order_date:,['CASH']] = cash_amount


    # Default the current portfolio value to the current amount of cash
    cur_portfolio_value = cash_amount

    # Calculate the remaining part of the portfolio value, by adding the price of all equities on that date
    for sym in lSymbols:
        cur_portfolio_value += df_Portfolio.loc[order_date,sym]*ldf_data.loc[order_date,sym]
        print "%4s value on %s is $%6s |%d shares" % (sym, order_date, ldf_data.loc[order_date, sym], df_Portfolio.loc[order_date,sym])
    
    df_Portfolio.loc[order_date:,["P_VALUE"]] = cur_portfolio_value


# Calculate the value of the portfolio on each trading day
for market_date in ldt_timestamps:
    cur_portfolio_value = float(df_Portfolio.loc[market_date,['CASH']])
    for sym in lSymbols:
        cur_portfolio_value += df_Portfolio.loc[market_date,sym]*ldf_data.loc[market_date,sym]
    
    df_Portfolio.loc[market_date,["P_VALUE"]] = cur_portfolio_value


print "\r\n"
print df_Portfolio

print "\r\nDetails on performance of the portfolio:","\r\n"
print "Data Range: ", startDate, "to", endDate,"\r\n"

# Calculating the statistics for our Fund
portfolio_cummulative_rets = np.array(df_Portfolio.loc[:,"P_VALUE"])
normalized_portfolio_rets = portfolio_cummulative_rets[:]/portfolio_cummulative_rets[0]

port_dreturns = tsu.returnize0(normalized_portfolio_rets)

# Standard Deviation of Fund
fDev = np.std(port_dreturns)
fMean = np.mean(port_dreturns)
fSharpe = (fMean*252)/(fDev*np.sqrt(252))

# Calculating statistics for the Market Portfolio ($SPX)
spx_cum_returns = np.array(spx_data[:])
norm_spx_rets = spx_cum_returns[:]/spx_cum_returns[0]

spx_drets = tsu.returnize0(norm_spx_rets)

fspxDev = np.std(spx_drets)
fspxMean = np.mean(spx_drets)
fspxSharpe = (fspxMean*252)/(fspxDev*np.sqrt(252))

print "Sharpe Ratio of Fund :",fSharpe
print "Sharpe Ratio of $SPX :", fspxSharpe,"\r\n"

print "Total Return of Fund :", portfolio_cummulative_rets[-1]/portfolio_cummulative_rets[0]
print "Total Return of $SPX :", spx_cum_returns[-1]/spx_cum_returns[0],"\r\n"

print "Standard Deviation of Fund :", fDev
print "Standard Deviation of SPX :", fspxDev,"\r\n"

print "Average Daily Return of Fund :", fMean
print "Average Daily Return of $SPX :", fspxMean,"\r\n"