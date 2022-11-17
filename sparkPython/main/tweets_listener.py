import tweepy
import json
from config.config import *
import socket

bearer_token = BEARER_TOKEN
search_term = "#Andrew Tate"

def twitterAuth():
  # create the authentication object
  authenticate = tweepy.OAuthHandler(API_KEY, API_KEY_SECRET)

  # set the access token and the access token secret
  authenticate.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
  return authenticate

class TweetListener(tweepy.StreamingClient):
  def __init__(self, csocket):
    self.client_socket = csocket

  def on_tweet(self, tweet):
    tw_dict = {}
    try:
      # read the Twitter data and then transform AS DICT
      tw_dict["ID"] = tweet.id
      tw_dict["text"] = tweet.text
      tw_dict["created_at"] = tweet.created_at
      # the 'text' in the JSON file contains the actual tweet.
      data = json.dumps(tw_dict, default=str)
      # the actual tweet data is sent to the client socket
      self.client_socket.send(data.encode('utf-8'))
      print("Sended ALL!!!")
      return True

    except BaseException as e:
      # Error handling
      print("Ahh! Look what is wrong : %s" % str(e))
      return True

  def on_error(self, status_code):
    print(status_code)
    return False

  def start_streaming_tweets(self, search_term):
    self.add_rules(tweepy.StreamRule(value="BTS lang:en"))
    self.sample(tweet_fields=["created_at"], expansions=["author_id"],user_fields=["username", "name"])
    

if __name__ == "__main__":

  def delete_all_rules(client, rules):
    if rules is None or rules.data is None:
        return None
    stream_rules = rules.data
    ids = list(map(lambda rule: rule.id, stream_rules))
    client.delete_rules(ids=ids)

  
  # create a socket object
  s = socket.socket()

  # Get local machine name : host and port
  host = "127.0.0.1"
  port = 3333

  # Bind to the port
  s.bind((host, port))
  print("Listening on port: %s" % str(port))

  # Wait and Establish the connection with client.
  s.listen(5)
  c, addr = s.accept()

  print("Received request from: " + str(addr))

  my_stream = TweetListener(bearer_token=BEARER_TOKEN, csocket=c)
  rules = my_stream.get_rules()

  # ALL LEGACY RULE DELETE.
  delete_all_rules(my_stream, rules)
  my_stream.start_streaming_tweets(search_term)

    

