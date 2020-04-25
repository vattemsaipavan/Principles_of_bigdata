from flask import Flask, send_file
import seaborn as sns
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas
from io import BytesIO
from wordcloud import WordCloud

app = Flask(__name__)


@app.route("/query/<id>", methods=['GET'])
def hello(id):
    if id == "1":
        query1 = spark.sql(
            "select place.country,count(*) As Count from tweet_table where place.country is not null GROUP BY "
            "place.country ORDER BY count "
            "DESC limit 10")
        pd_query1 = query1.toPandas()
        pd_query1.plot.pie(y='Count', labels=pd_query1.country.values.tolist(), figsize=(10, 5), autopct='%.2f')
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "2":
        query2 = spark.sql(
            "select user.name,retweeted_status.text as Retweet_Text,retweeted_status.retweet_count as Retweet_Count "
            "from tweet_table where retweeted_status.retweet_count is not null order by "
            "retweeted_status.retweet_count desc limit 10")
        pd_query2 = query2.toPandas()
        pd_query2.plot()
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "3":
        query3 = spark.sql("select user.location,count(text) as tweet_count from tweet_table where "
                           "place.country='United States'and "
                           "user.location is not null Group By user.location ORDER BY tweet_count DESC LIMIT 15")
        pd_query3 = query3.toPandas()
        pd_query3.plot.area(x="location", y="tweet_count", figsize=(11, 5))
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "4":
        query4 = spark.sql("select user.screen_name,text,retweeted_status.retweet_count from tweet_table order by "
                           "retweeted_status.retweet_count DESC limit 10")
        pd_query4 = query4.toPandas()
        pd_query4.plot.scatter(x="retweet_count", y='screen_name', color='DarkBlue', s=50, figsize=(5,5))
        # plt.title("Users with more no of retweets for his tweet")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "5":
        query5 = spark.sql(
            "select count(*) as count,q.text from (select case when text like '%fcb%' then 'fc barcelona' when "
            " text like '%real madrid%' then 'real madrid' when text like '%fifa%' then 'fifa' when text"
            " like '%la liga%' then 'la liga' when text like '%nfl%' then 'nfl' when text like '%arsenal%' "
            " then 'arsenal' when text like '%chelsea%' then 'chelsea fc' when text like '%manchester%' "
            "then 'manchester' when text like '%psg%' then 'psg' when text like '%premier league%' then 'premier "
            "league' "
            " end as text from "
            " tweet_table)q group by q.text")
        pd_query5 = query5.toPandas()
        sns.catplot(x="text", y="count", kind="swarm", data=pd_query5.dropna(),aspect=3)
        # plt.title("Tweets based on the different league matches")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "6":
        query6 = spark.sql(
            "select user.screen_name, max(user.followers_count)as followers_count from tweet_table where text "
            " like '%football%' group by user.screen_name, user.lang order by followers_count desc limit 10")
        pd_query6 = query6.toPandas()
        sns.catplot(x="screen_name", y="followers_count", kind="violin", data=pd_query6, height=5, aspect=3)
        # plt.title("User with more no of followers")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "7":
        query7 = spark.sql(
            "select count(*) as NumberOfTweets, 'Android' as Source from tweet_table where source like '%Twitter for "
            "Android%' UNION select count() as NumberOfTweets, 'IPhone' as Source from tweet_table where source like "
            "'%Twitter for iPhone%' UNION select count() as NumberOfTweets, 'IPad' as Source from tweet_table where "
            "source like '%Twitter for iPad%' UNION select count() as NumberOfTweets, 'Web' as Source from "
            "tweet_table where source like '%Twitter Web App%'")
        pd_query7 = query7.toPandas()
        pd_query7.plot.line(y="NumberOfTweets", x="Source")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "8":
        day_data = spark.sql("SELECT substring(user.created_at,1,3) as day from tweet_table where text is not null")
        day_data.createOrReplaceTempView("day_data")
        days_final = spark.sql(
            """ SELECT Case
              when day LIKE '%Mon%' then 'WEEKDAY'
              when day LIKE '%Tue%' then 'WEEKDAY'
              when day LIKE '%Wed%' then 'WEEKDAY'
              when day LIKE '%Thu%' then 'WEEKDAY'
              when day LIKE '%Fri%' then 'WEEKDAY'
              when day LIKE '%Sat%' then 'WEEKEND'
              when day LIKE '%Sun%' then 'WEEKEND'
               else
               null
               end as day1 from day_data where day is not null""")
        days_final.createOrReplaceTempView("days_final")
        query8 = spark.sql("SELECT day1 as Day,Count(*) as Day_Count from days_final where day1 is not null group by "
                           "day1 order by count(*) desc")
        pd_query8 = query8.toPandas()
        pd_query8.plot.pie(y="Day_Count", labels=pd_query8.Day.tolist(), autopct='%.2f')
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "9":
        query9 = spark.sql("select count(*) as count,lang from tweet_table where text like '%la liga%' group by lang "
                           "order by count desc")
        pd_query9 = query9.toPandas()
        sns.catplot(x="lang", y="count", data=pd_query9)
        # plt.title("Tweet count based on language")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "10":
        timehour = spark.sql("SELECT SUBSTRING(created_at,12,2) as hour from tweet_table where text is not null")

        timehour.createOrReplaceTempView("timehour")
        timeAnalysis = spark.sql(
            "SELECT Case when hour>=0 and hour <4 then 'midnight' when hour>=4 and hour <7 then 'earlymorning' "
            " when hour>=7 and hour <12 then 'Day-time' when hour>=12 and hour <15 then 'afternoon' when hour>=15 and "
            " hour <18 then 'evening' when hour>=18 and hour <=23 then 'Night-time' end as time from timehour")

        timeAnalysis.createOrReplaceTempView("timeAnalysis")
        query10 = spark.sql("SELECT time as hour,Count(*) as tweets_count from timeAnalysis where time is not null "
                            "group by time order by count(*) desc")
        pd_query10 = query10.toPandas()
        sns.catplot(x="hour", y="tweets_count", kind="violin", style="smoker", split=True, data=pd_query10)
        # plt.title("Tweet count on hourly bases")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "11":
        query11 = spark.sql(
            "select count(*) as count,text from tweet_table where text like '%football%' group by text order by count "
            "desc limit 20")
        pdquery11 = query11.toPandas()
        train_text = " ".join(pdquery11.text)
        wordcloud = WordCloud().generate(train_text)
        plt.figure()
        plt.subplots(figsize=(50, 50))
        wordcloud = WordCloud(
            background_color="Black",
            max_words=len(train_text),
            max_font_size=30,
            relative_scaling=.5).generate(train_text)
        plt.imshow(wordcloud)
        plt.axis("off")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Phase 2 querying and plotting").getOrCreate()
    sc = spark.sparkContext
    df = spark.read.json(r"F:\Twitter\account_and_user\tweets\football_data.json")
    df.createOrReplaceTempView("tweet_table")
    app.run(debug=True)
