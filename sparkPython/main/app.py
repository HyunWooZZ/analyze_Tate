import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, FloatType
from pyspark.sql import functions as F
from pyspark.sql.functions import *

from textblob import TextBlob


# Create a function to get the subjectifvity
def getSubjectivity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.subjectivity

# Create a function to get the polarity
def getPolarity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.polarity

def getSentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'
    else:
        return 'Positive'

def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    # remove users
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # remove puntuation
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

    # remove number
    tweet = re.sub('([0-9]+)', '', str(tweet))

    # remove hashtag
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    return tweet


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
    clean_tweets = F.udf(cleanTweet, StringType())
    raw_tweets = df1.withColumn('processed_text', clean_tweets(col("text")))

    subjectivity = F.udf(getSubjectivity, FloatType())
    polarity = F.udf(getPolarity, FloatType())
    sentiment = F.udf(getSentiment, StringType())

    subjectivity_tweets = raw_tweets.withColumn('subjectivity', subjectivity(col("processed_text")))
    polarity_tweets = subjectivity_tweets.withColumn("polarity", polarity(col("processed_text")))
    sentiment_tweets = polarity_tweets.withColumn("sentiment", sentiment(col("polarity")))

    writeTweet = sentiment_tweets.writeStream. \
    outputMode("append"). \
    format("console"). \
    queryName("tweetquery"). \
    trigger(processingTime='2 seconds'). \
    start()



