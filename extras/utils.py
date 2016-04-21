from configparser import ConfigParser
from pymongo import MongoClient
from math import pow, sqrt

__author__ = 'MuhamadNoorZainal MuhamadZabidi'
__version__ = '1.5'


class Database(object):
    def __init__(self, col_name):
        self.client = MongoClient()
        self.db = self.client['Tweets']

        if col_name is None:
            self.col = self.db['final_db']
        else:
            self.col = self.db[col_name]


def secs_to_hrs(seconds, process):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)

    if d > 0:
        print(process + ' {} days: {} hours: {} mins: {:.2f} secs'.format(d, h, m, s))
    else:
        print(process + ' {} hours: {} mins: {:.2f} secs'.format(h, m, s))


def get_params(config_filename='./extras/config.ini', **kwargs):
    config_object = {}
    if config_filename is not None:
        config_file = ConfigParser()
        config_file.read(config_filename)

        if kwargs is not None:
            if kwargs.pop('mongo', False) is True:
                mongo_dict = {'db_name': config_file['MONGO_PARAMETERS']['db_name'],
                              'col_name': config_file['MONGO_PARAMETERS']['col_name']}

                config_object.update(mongo_dict)

            if kwargs.pop('tw_auth', False) is True:
                tw_auth_dict = {'cons_key': config_file['TWITTER_STREAM_AUTH']['consumer_key'],
                                'cons_sec': config_file['TWITTER_STREAM_AUTH']['consumer_secret'],
                                'acc_token': config_file['TWITTER_STREAM_AUTH']['access_token'],
                                'acc_token_sec': config_file['TWITTER_STREAM_AUTH']['access_token_secret']}

                config_object.update(tw_auth_dict)

            if kwargs.pop('tw_user_auth', False) is True:
                tw_auth_dict = {'cons_key': config_file['TWITTER_USER_TWEETS_AUTH']['consumer_key'],
                                'cons_sec': config_file['TWITTER_USER_TWEETS_AUTH']['consumer_secret'],
                                'acc_token': config_file['TWITTER_USER_TWEETS_AUTH']['access_token'],
                                'acc_token_sec': config_file['TWITTER_USER_TWEETS_AUTH']['access_token_secret']}

                config_object.update(tw_auth_dict)

            if kwargs.pop('lda', False) is True:
                lda_dict = {'corpus_file': config_file['LDA_FILES']['corpus_filename'],
                            'dict_file': config_file['LDA_FILES']['dict_filename'],
                            'lda_file': config_file['LDA_FILES']['lda_filename'],

                            'num_topics': int(config_file['LDA_PARAMETERS']['num_topics']),
                            'workers': int(config_file['LDA_PARAMETERS']['workers']),
                            'chunks': int(config_file['LDA_PARAMETERS']['chunks']),
                            'passes': int(config_file['LDA_PARAMETERS']['passes']),
                            'alpha': config_file['LDA_PARAMETERS']['alpha']}

                config_object.update(lda_dict)

    return config_object


def cosine_similarity(a, b):
    try:
        result = dot_product(a, b) / magnitude(a, b)
    except ZeroDivisionError:
        result = 0

    return result


def dot_product(a, b):
    dot = 0
    common_items = list(set(a.keys()) & set(b.keys()))

    for k in common_items:
        dot += a[k] * b[k]

    return dot


def magnitude(a, b):
    mgn_a = 0
    mgn_b = 0

    for v in a.values():
        mgn_a += pow(v, 2)

    for v in b.values():
        mgn_b += pow(v, 2)

    return sqrt(mgn_a) * sqrt(mgn_b)


def normalize_counts(x):
    count_total = sum(x.values())

    for k in x.keys():
        x[k] = x[k] / count_total

    return x


def reset_tweets(db_name='final_db'):
    _mongo = Database(db_name)

    cursor = _mongo.col.find()
    update = {'$set': {'tokens': [],
                       'topics': [],
                       'doc_sim': [],
                       'tokenized': False}}

    for doc in cursor:
        _mongo.col.update_one({'_id': doc['_id']})
