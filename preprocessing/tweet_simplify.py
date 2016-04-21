"""
This script:

* Is used to simplify the tweet retrieved from server
* Removes URLs from text
* Extracts only the useful information
* And stores them all to a new dictionary

Author: Muhamad Noor Zainal MUHAMAD ZABIDI
Python 3.5
"""

from preprocessing.lda_prep import strip_urls

__version__ = '3.0'

"""
----------------------------------------------------------------------------------
                                    Functions
----------------------------------------------------------------------------------
"""


def get_hashtags(hashtag_entity):
    """
        - gets all the hashtags if available in the tweet
    """
    hashtags = []
    if hashtag_entity is not None:
        for hashtag in hashtag_entity:
            hashtags.append(hashtag['text'])

    return hashtags


def get_mentions(mention_entity):
    """
        - gets the user mentions and their string ids if available
    """
    mentions = {'screen_names': [], 'string_id': []}

    if mention_entity is not None:
        for user in mention_entity:
            mentions['screen_names'].append(user['screen_name'])
            mentions['string_id'].append(user['id_str'])

    return mentions


def get_source(tweet):
    """
        - retrieves the source of the tweet
        - simplifies it as the text is a long http formatted string
    """
    source = ""
    source_choice = {'a': 'Web',
                     'b': 'Android',
                     'c': 'iPhone',
                     'd': 'Foursquare'}

    for k in source_choice:
        if source_choice.get(k) in tweet['source']:
            source = source_choice.get(k)
            break
        else:
            source = 'Others'

    return source


"""
----------------------------------------------------------------------------------
                                     Simplify
----------------------------------------------------------------------------------
"""


def simplify(tweet):
    """
        simplify the tweet data that we get from server

        :param tweet: the tweet object that has been retrieved from server
        :return new tweet: the new simplified tweet object
    """

    new_tweet = {'created': tweet['created_at'],
                 'user_id_str': tweet['user']['id_str'],
                 'screen_name': tweet['user']['screen_name'],
                 'lang_id': tweet['lang'],
                 'text': strip_urls(tweet['text']),
                 'hashtags': get_hashtags(tweet['entities']['hashtags']),
                 'mentions': get_mentions(tweet['entities']['user_mentions']),
                 'retweeted': tweet['retweeted'],
                 'in_reply_to_user_id_str': tweet['in_reply_to_user_id_str'],
                 'source': get_source(tweet),
                 'place': tweet['place'],
                 'tokens': [],
                 'tokenized': False}

    if new_tweet['place'] is not None:
        if new_tweet['place']['url'] is not None:
            new_tweet['place'].pop('url', None)

    return new_tweet








