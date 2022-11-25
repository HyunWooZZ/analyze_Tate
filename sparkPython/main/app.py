import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, FloatType
from pyspark.sql import functions as F
from pyspark.sql.functions import *

from textblob import TextBlob


TRESHOLD = 0.3


# Create a function to get the polaritys
def getPolarity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.polarity

# Create a function to get sentimental category
def getSentiment(polarityValue: int) -> str:
    if polarityValue < (-TRESHOLD):
        return 'Negative'
    elif polarityValue > TRESHOLD:
        return 'Positive'
    else:
        return "Neutral"

# Clean the tweet
def clean_tweet(tweet):
    r = tweet.lower()
    r = re.sub("'", "", r) # This is to avoid removing contractions in english₩``
    r = re.sub("@[A-Za-z0-9_]+","", r)
    r = re.sub("#[A-Za-z0-9_]+","", r)
    r = re.sub(r'http\S+', '', r)
    r = re.sub('[()!?]', ' ', r)
    r = re.sub('\[.*?\]',' ', r)
    r = re.sub("[^a-z0-9]"," ", r)
    r = r.split()
    stopwords = ["for", "on", "an", "a", "of", "and", "in", "the", "to", "from"]
    r = [w for w in r if not w in stopwords]
    r = " ".join(word for word in r)
    return r


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("TwitterSentimentAnalysis")\
        .master("local[*]")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    tweet_schema = StructType().add("ID", "string").add("text", "string").add("created_at", "string")

    df = spark.readStream\
        .format("socket")\
        .option("host", "127.0.0.1")\
        .option("port", 3333)\
        .load()

    
    values = df.select(from_json(df.value.cast("string"), tweet_schema).alias("tweet"))

    
    df1 = values.select("tweet.*")

    # Clean tweet
    clean_tweets = F.udf(clean_tweet, StringType())
    raw_tweets = df1.withColumn('processed_text', clean_tweets(col("text")))

    # Classify all tweet by sentiment treshhold
    polarity = F.udf(getPolarity, FloatType())
    sentiment = F.udf(getSentiment, StringType())
    polarity_tweets = raw_tweets.withColumn("polarity", polarity(col("processed_text")))
    sentiment_tweets = polarity_tweets.withColumn("sentiment", sentiment(col("polarity")))


    window_tweets = sentiment_tweets.select("*")\
    .groupby(window(sentiment_tweets.created_at, "60 seconds"), sentiment_tweets.sentiment) \
    .agg(count("*").alias("numEvents"))



    writeTweet = window_tweets.writeStream. \
    outputMode("update"). \
    format("console"). \
    queryName("tweetquery"). \
    start()

    writeTweet.awaitTermination()


    spark.stop()