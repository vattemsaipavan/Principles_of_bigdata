from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
from multiprocessing import Pool,Process

CONSUMER_KEY = "O5SqraTheNJEnkBQD940XTgOl"
CONSUMER_SECRET = "TatQ7IjZoOoc5w79XPokFyZRIKh0H0lCZeKfWbi87CniVEeOOg"
ACCESS_TOKEN = "3061266102-bDKeJI29pbbLFDeR2pU5D6lMOTWExeh6im6K23e"
ACCESS_TOKEN_SECRET = "AZ8b5sW0lApkF3qTr4ITJhJ8DMz3WCLLaOwuHlZZrxvln"


# # # # TWITTER STREAMER # # # #
class TwitterStreamer():
    """
    Class for streaming and processing live tweets.
    """

    def __init__(self):
        pass

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list):
        # This handles Twitter authetification and the connection to Twitter Streaming API
        listener = StdOutListener(fetched_tweets_filename)
        auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        stream = Stream(auth, listener)

        # This line filter Twitter Streams to capture data by the keywords:
        stream.filter(track=hash_tag_list)


# # # # TWITTER STREAM LISTENER # # # #
class StdOutListener(StreamListener):
    """
    This is a basic listener that just prints received tweets to stdout.
    """

    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        try:
            print data
            with open(self.fetched_tweets_filename, 'a+') as tf:
                tf.write(data)
            return True
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    # Authenticate using config.py and connect to Twitter Streaming API.
    hash_tag_list = [ "la liga", "santander","football","messi", "leo","ronaldo","cristiano",
                      "cr7","goat","iefa","best player","golden shoes","awards","fifa","fcb",
                      "soccer","calcio","nfl","footballseason","footballgame","real madrid",
                      "fc barcelona","psg","hala madrid"]
    fetched_tweets_filename = "football_data.json"
    twitter_streamer = TwitterStreamer()

    # pool = Pool(processes=4)
    # p = Process(target=twitter_streamer.stream_tweets(fetched_tweets_filename, hash_tag_list))
    # p.start()
    # print pool.map(twitter_streamer.stream_tweets(fetched_tweets_filename, hash_tag_list),range(900000))
    twitter_streamer.stream_tweets(fetched_tweets_filename, hash_tag_list)