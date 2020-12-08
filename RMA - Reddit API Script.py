import praw
import pprint
import pandas as pd
import pyodbc
import pytz as tz
from datetime import datetime
from time import sleep
import smtplib, ssl
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

#Reddit API details
reddit = praw.Reddit(client_id = '[Client ID Here]',
client_secret = '[Client Secret Here]',
user_agent = '[User Agent Here]')

#SMPT Details
port = 465
context = ssl.create_default_context()
senderEmail = '[Sender Email Address Here]'
receiverEmail = '[Receiver Email Address Here]'

#PREPARATION
#create an array of subreddits for analysis
subredditList = [
    "news", "worldnews", "truenews", "indepthstories",
    "politics", "politicaldiscussion", "geopolitics",
    "investing", "securityanalysis", "stockmarket", "business",
    "economics", "finance", "options", "wallstreetbets"
]

#create a dataframe to hold posts and sentiment scores
postDF = pd.DataFrame(columns=['LoadDate','Subreddit','ID','Title','Score','UpvoteRatio','URL','Permalink','CreatedAt','CreatedAtUTC','Author','LinkFlairText','SelfText','CommentCount','TitleNeg','TitleNeu','TitlePos','TitleCompSent','SelftextNeg','SelftextNeu','SelftextPos','SelftextCompSent'
])

#sentiment analysis function
analyzer = SentimentIntensityAnalyzer() 

#get current date for LoadDate column
loadDate = datetime.now()

#TRY / EXCEPT
try:
    #create SQL connection to RMA database
    sql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=[Server Here];DATABASE=[Database Here]; Trusted_Connection=yes')

    #create a dataframe to hold post IDs of posts already in database so they can be excluded from scraping
    idQuery = "SELECT DISTINCT ID FROM RMA.dbo.RedditPosts"
    existingIdDF = pd.read_sql(idQuery, sql_conn)

    #SCRAPING
    #loop through each subreddit in the array and load it for processing
    for aSubreddit in subredditList:
        loadedSubreddit = reddit.subreddit(aSubreddit)
        
        #loop through posts from each subreddit, check if they are already in database, if not - calculate sentiment score and append to post dataframe
        #each of the percentage columns indicate how much of the review is negative, neutral, or positive
        #the compoundsentiment column is a range from -1 to 1 with 1 being the most positive review possible and vice versa
        for submission in loadedSubreddit.hot(limit=1000):
            if submission.id not in existingIdDF.values:
                #score the submission titles
                titleScore = analyzer.polarity_scores(submission.title)
                titleNeg = titleScore['neg'] 
                titleNeu = titleScore['neu']
                titlePos = titleScore['pos']
                titleCompsent = titleScore['compound']
                #score the submission selftexts
                selftextScore = analyzer.polarity_scores(submission.selftext) 
                selftextNeg = selftextScore['neg'] 
                selftextNeu = selftextScore['neu']
                selftextPos = selftextScore['pos']
                selftextCompsent = selftextScore['compound']
            
                createdAt = datetime.fromtimestamp(submission.created_utc) #convert UTC timestamp to local datetime
                createdAtUTC = datetime.utcfromtimestamp(submission.created_utc) #convert UTC timestamp to UTC datetime
                subredditString = str(submission.subreddit) #convert the "Subreddit" type to a string type for use in dataframe
                authorString = str(submission.author) #convert the "Redditor" type to a string type for use in dataframe

                #append to data frame            
                postDF = postDF.append({
                    'LoadDate': loadDate
                    ,'Subreddit': subredditString
                    ,'ID': submission.id            
                    ,'Title': submission.title 
                    ,'Score': submission.score
                    ,'UpvoteRatio': submission.upvote_ratio
                    ,'URL': submission.url
                    ,'Permalink': submission.permalink
                    ,'CreatedAt': createdAt
                    ,'CreatedAtUTC': createdAtUTC
                    ,'Author': authorString          
                    ,'LinkFlairText': submission.link_flair_text            
                    ,'SelfText': submission.selftext
                    ,'CommentCount': submission.num_comments
                    ,'TitleNeg': titleNeg
                    ,'TitleNeu': titleNeu
                    ,'TitlePos': titlePos
                    ,'TitleCompSent': titleCompsent
                    ,'SelftextNeg': selftextNeg
                    ,'SelftextNeu': selftextNeu
                    ,'SelftextPos': selftextPos
                    ,'SelftextCompSent': selftextCompsent
                    } , ignore_index=True)
                    
                print(submission.title + " - added to the database")
                sleep(0.1) #sleep for 1/10th of a second to limit API request rate
            #else: print(submission.title + "- is already in the database")

    
    #LOAD TO DATABASE
    #create cursor for input to database
    cursor = sql_conn.cursor()
    #insert each row of the post dataframe into the RedditPosts table
    for index, row in postDF.iterrows():
        cursor.execute("""INSERT INTO RMA.dbo.RedditPosts(LoadDate,Subreddit,ID,Title,Score,UpvoteRatio,URL,Permalink,CreatedAt,CreatedAtUTC,Author,LinkFlairText,SelfText,CommentCount,TitleNeg,TitleNeu,TitlePos,TitleCompSent,SelftextNeg,SelftextNeu,SelftextPos,SelftextCompSent) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        ,row['LoadDate']
        ,row['Subreddit']
        ,row['ID']
        ,row['Title']
        ,row['Score']
        ,row['UpvoteRatio']
        ,row['URL']
        ,row['Permalink']
        ,row['CreatedAt']
        ,row['CreatedAtUTC']
        ,row['Author']
        ,row['LinkFlairText']
        ,row['SelfText']
        ,row['CommentCount']
        ,row['TitleNeg']
        ,row['TitleNeu']
        ,row['TitlePos']
        ,row['TitleCompSent']
        ,row['SelftextNeg']
        ,row['SelftextNeu']
        ,row['SelftextPos']
        ,row['SelftextCompSent']
        )
    sql_conn.commit()
    cursor.close()
    sql_conn.close()
  
except:
    #Create SMTP connection and send failure email    
    failureMessage = "Subject: Reddit Scraper - FAILURE \n\nThis message was sent via Python."
    with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
        server.login("[Sender Email Here]", "[Sender Email Password Here]")
        server.sendmail(senderEmail, receiverEmail, failureMessage)

else:
    #Get a row count from the post dataframe
    rows = len(postDF.index)

    #Create SMTP connection and send success email    
    successMessage = "Subject: Reddit Scraper - SUCCESS \n\n{} rows have been inserted. \n\nThis message was sent via Python.".format(rows)
    with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
        server.login("[Sender Email Here]", "[Sender Email Password Here]")
        server.sendmail(senderEmail, receiverEmail, successMessage)