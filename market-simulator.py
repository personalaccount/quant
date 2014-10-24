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
#orders_csv_filename = "orders-short.csv"
orders_csv_filename = "orders.csv"
cash_amount = 1000000

# Create a numpy array of orders
na_orders_list = np.loadtxt(orders_csv_filename, dtype={'names':('year', 'month', 'day', 'symbol', 'order_type', 'num_shares'), 
    'formats':('i4','i2','i2','S4','S4','i4')}, delimiter=',',skiprows=0,)

num_of_orders = len(na_orders_list)
last_row = num_of_orders-1

# Create a list of symbols
lSymbols = []
lDates = []

for i in range (num_of_orders):
    # Fill-in the list of Dates
    order_date = dt.datetime(na_orders_list[i]['year'],na_orders_list[i]['month'],na_orders_list[i]['day']) + dt.timedelta(hours=16)
    if (order_date not in lDates):
        lDates.append(order_date)

    # Fill-in the list of Symbols
    if (na_orders_list[i]['symbol'] not in lSymbols):
        lSymbols.append(na_orders_list[i]['symbol'])

# Set the start date
startDate = dt.datetime(na_orders_list[0]['year'], na_orders_list[0]['month'], na_orders_list[0]['day'])

# End date should be offset-ed by 1 day in order to read the adjusted_close for the last date. 
endDate = dt.datetime(na_orders_list[last_row]['year'], na_orders_list[last_row]['month'], na_orders_list[last_row]['day'])+dt.timedelta(days=1)

print "Start date: ", startDate
print "End date: ", endDate
print "Cash on hand: $", cash_amount

# Specify 16:00 hours to read the data that was available at the close of the trading day
dt_timeofday = dt.timedelta(hours=16)

# The getNYSEdays() function tells if the market was open on a particular day
# Get a list of timestamps that represent NYSE closing times between the start and end dates
ldt_timestamps = du.getNYSEdays(startDate, endDate, dt_timeofday)

# Create an empty pandas dataframe for the portfolio
# and fill it with 0
df_Portfolio = pd.DataFrame(index=lDates, columns=lSymbols)
df_Portfolio = df_Portfolio.fillna(0)

#print df_Portfolio

# Create an object that will be ready to read from Yahoo data source
c_dataobj = da.DataAccess('Yahoo', cachestalltime=0)

# Create a list of dataframe objects which have all the different types of data
ldf_data = c_dataobj.get_data(ldt_timestamps, lSymbols, 'actual_close')

# Now we have the orders, sorted by date and we have the prices.
# Iterate over the orders, check the prices and update the array of cash.

for j in range(0,num_of_orders):
    order_date = dt.datetime(na_orders_list[j]['year'],na_orders_list[j]['month'],na_orders_list[j]['day']) + dt.timedelta(hours=16)
    symbol = na_orders_list[j]['symbol']
    num_shares = na_orders_list[j]['num_shares']
    order_type = na_orders_list[j]['order_type']
    # The .loc attribute is the primary access method.
    # Similarly to loc, "at" provides label based scalar lookups, while, "iat" provides integer based lookups analogously to iloc    
    share_price = ldf_data.loc[order_date,symbol]
    order_value = num_shares*share_price

    print "\r\nOrder date: ", order_date
    print "Processing: ",order_type," ",num_shares," shares of ",symbol
    print symbol," share market price is $",share_price
    print "Total order amount: $", order_value

    current_num_shares = df_Portfolio.at[order_date,symbol]

    if order_type == "Buy":
        cash_amount = cash_amount - order_value
        new_num_shares = current_num_shares + num_shares

    if order_type == "Sell":
        cash_amount = cash_amount + order_value
        new_num_shares = current_num_shares - num_shares
    
    df_Portfolio.loc[order_date:,[symbol]] = new_num_shares
    print "Current cash on hand $",cash_amount
    print df_Portfolio           

print "\r\n Final result:"
print "$", cash_amount
print df_Portfolio