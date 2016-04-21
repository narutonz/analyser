"""
This script:

* Is used to retrieve tweets from the streaming API
    - Only takes tweets that are geo-tagged and in UK
* Can also be used to download tweets of specific users if given
    Twitter ID or screen name
* After simplification of the tweet it is stored in MongoDB
* If run using console then the program will automatically download
    tweets from stream

Author: Muhamad Noor Zainal MUHAMAD ZABIDI
Python 3.5
"""

import json
from datetime import datetime

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.api import API
from tweepy.streaming import StreamListener

import analyser.extras.utils as ut
from analyser.preprocessing.tweet_simplify import simplify

__version__ = '4.5'


class MyCustomListener(StreamListener):
    def on_data(self, data):
        try:
            data = json.loads(data)
            try:
                tweet_simple = simplify(data)
                print(tweet_simple)
            except KeyError:
                pass
            return True

        except BaseException as error:
            print(str(datetime.now()) + 'Failed on data: ' + str(error))

    def on_error(self, status_code):
        print(str(datetime.now()) + ': Status Code: ' + str(status_code))
        return True  # Don't kill the stream

    def on_timeout(self):
        print(str(datetime.now()) + ': Timeout.')
        return True  # Don't kill the stream


def download_stream():
    # get parameters from config file and configure logger
    auth_params = ut.get_params(tw_auth=True)

    auth = OAuthHandler(auth_params['cons_key'],
                        auth_params['cons_sec'])
    auth.set_access_token(auth_params['acc_token'],
                          auth_params['acc_token_sec'])
    auth.secure = True
    api = API(auth)

    # if the authentication was successful my name should be printed
    # 'muhamad noor zainal'
    print(api.me().name)

    # start the stream listener, filter for tweets only in UK
    stream = Stream(auth, MyCustomListener())
    stream.filter(locations=[-6.38, 49.87, 1.77, 55.81], async=True)


def download_user_tweets(screen_name, tw_id):
    """ https://gist.github.com/yanofsky/5436496#file-tweet_dumper-py """
    # Twitter only allows access to a users most recent 3240 tweets with this method

    params = ut.get_params(tw_user_auth=True)

    # authorize twitter, initialize tweepy
    auth = OAuthHandler(consumer_key=params['cons_key'],
                        consumer_secret=params['cons_sec'])
    auth.set_access_token(key=params['acc_token'],
                          secret=params['acc_token_sec'])
    api = API(auth)

    # initialize a list to hold all the tweepy Tweets
    all_tweets = []

    if screen_name is not None:
        # make initial request for most recent tweets (200 is the maximum allowed count)
        new_tweets = api.user_timeline(screen_name=screen_name, count=200)
    else:
        new_tweets = api.user_timeline(user_id=tw_id, count=200)

    # save most recent tweets
    all_tweets.extend(new_tweets)

    # save the id of the oldest tweet less one
    oldest = all_tweets[-1].id - 1

    # keep grabbing tweets until there are no tweets left to grab
    while len(all_tweets) < 500 and len(new_tweets) > 0:
        # print("getting tweets before %s" % oldest)

        # all subsequent requests use the max_id param to prevent duplicates
        if screen_name is not None:
            new_tweets = api.user_timeline(screen_name=screen_name, count=200, max_id=oldest)

        elif tw_id is not None:
            new_tweets = api.user_timeline(user_id=tw_id, count=200, max_id=oldest)

        # save most recent tweets
        all_tweets.extend(new_tweets)

        # update the id of the oldest tweet less one
        oldest = all_tweets[-1].id - 1

        # print("...%s tweets downloaded so far" % (len(all_tweets)))

    return all_tweets


if __name__ == '__main__':
    mongo_ = ut.Database(None)
    download_stream()