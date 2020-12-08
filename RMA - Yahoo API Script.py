import pandas as pd
import yfinance as yf
import pyodbc
import time
from datetime import datetime, timedelta

#create SQL connection to RMA database
sql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=[*Server Here*];DATABASE=[*Database Here*]; Trusted_Connection=yes')

#create prices dataframe
priceDF = pd.DataFrame(columns=['Symbol','Open','High','Low','Close','Volume'])
#create identical temp prices dataframe for purposes of ticker loop
tempDF = pd.DataFrame(columns=['Symbol','Open','High','Low','Close','Volume'])

#create a dataframe to hold ticker symbols queried from SP500 table
tickerQuery = "SELECT Symbol FROM RMA.dbo.SP500"
tickerList = pd.read_sql(tickerQuery, sql_conn)

#define range of dates from which to download prices
#startDate: get date of one day after the most recent stock price data in the database
startDateQuery = "SELECT DATEADD(day,1,CONVERT(date,MAX(Datetime))) FROM RMA.dbo.StockPrices"
startDate = pd.read_sql(startDateQuery, sql_conn)
#get first row of returned dataframe, convert to a string, and strip the whitespace
startDate = startDate.iloc[0].to_string().strip()
#endDate: one day before current day (so a full day of prices is always downloaded)
endDate = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d') 

#loop through the symbols in tickerList and download the stockprice data
for index, row in tickerList.iterrows():

    #load a symbol
    aSymbol = row['Symbol']

    #clear temp prices dataframe
    tempDF.iloc[0:0]
    
    #download prices for current symbol and populate dataframe
    tempDF = yf.download(
        aSymbol
        , start = startDate
        , end =  endDate
        , interval= '5m'
        , auto_adjust = True
        , prepost = True
        )

    #save index (datetimes) to a column
    tempDF.reset_index(level=0, inplace=True)

    #append ticker symbol to temp dataframe 
    tempDF['Symbol'] = aSymbol
    
    #append temp dataframe to priceDF
    priceDF = priceDF.append(tempDF)

    #print symbol
    print(aSymbol + " - added to the dataframe")
    #print(priceDF)

    #wait 1.5 seconds to avoid overload of API requests
    time.sleep(1.5)

#round all price columns to a consistent decimal place
priceDF = priceDF.round({'Open': 4,'High': 4,'Low': 4,'Close': 4})

#convert price columns to floats
priceDF = priceDF.astype({'Open': float,'High': float,'Low': float,'Close': float})

#output to CSV for backup
priceDF.to_csv(r'C:\Users\HP\Documents\prices ' + startDate + ' to ' + endDate + '.csv',index = False)

#LOAD TO DATABASE
#create cursor for input to database
cursor = sql_conn.cursor()

#insert each row of the post dataframe into the RedditPosts table
for index, row in priceDF.iterrows():
    cursor.execute("""INSERT INTO RMA.dbo.StockPrices (Symbol,[Open],High,Low,[Close],Volume,Datetime) values(?,?,?,?,?,?,?)"""
    ,row['Symbol']
    ,row['Open']
    ,row['High']
    ,row['Low']
    ,row['Close']
    ,row['Volume']
    ,row['Datetime']        
    )
sql_conn.commit()

#Delete any current-day data that may have been extraneously added to the database
cursor.execute("""DELETE FROM rma.dbo.StockPrices WHERE datetime >= CONVERT(date,GETDATE())""")
sql_conn.commit()

cursor.close()
sql_conn.close()

