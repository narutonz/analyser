import re
from string import punctuation
from nltk.tokenize.casual import TweetTokenizer
from nltk.corpus import stopwords
from gensim import corpora
from guess_language import guess_language


__version__ = '6.0'

"""
----------------------------------------------------------------------------------
                               Clean Text & Tokenize
----------------------------------------------------------------------------------
"""


def strip_urls(text):
    """ removes urls """
    return re.sub(r'(?:\@|http[s]?\://)\S+', "", text)


def strip_punctuation(text):
    """ removes punctuation """
    re_punct = re.compile('[%s]' % re.escape(punctuation))
    return re_punct.sub('', text)


def strip_numerics(text):
    """" removes numeric words """
    re_numeric = re.compile(r'[0-9]+', re.UNICODE)
    return re_numeric.sub('', text)


def strip_short(text, minlength=3):
    """ removes words with length below 3 """
    return " ".join(w for w in text.split() if len(w) >= minlength)


def tokenize(text):
    """ tokenizes the text simultaneously removing handles """
    tokenizer = TweetTokenizer(preserve_case=False, strip_handles=True,
                               reduce_len=True)
    return tokenizer.tokenize(text)


def stop_words_list():
    stop_words = """
        a about above across after afterwards again against all almost alone along already also although always am among
        amongst amoungst amount an and another any anyhow anyone anything anyway anywhere are around as at amp
        back be became because become becomes becoming been before beforehand behind being below beside besides between
        beyond bill both bottom but by bitch bastard
        call can cannot cant co computer con could couldnt couldn't cry
        de describe detail did didn do does doesn doing don done down due during
        each eg eight either eleven else elsewhere empty enough etc even ever every everyone everything everywhere except
        few fifteen fify fill find fire first five for former formerly forty found four from front full further fuck ffs
        get give go got
        had has hasnt have he hence her here hereafter hereby herein hereupon hers herself him himself his
        how however hundred hmm hey hoo
        i ie ive if in inc indeed interest into is it its itself iep
        just
        keep kg km
        last latter latterly least less ltd lol let
        made make many may me meanwhile might mill mine more moreover most mostly move much must my myself
        name namely neither never nevertheless next nine no nobody none noone nor not nothing now nowhere
        of off often on once one only onto or other others otherwise our ours ourselves out over own okay
        ooo
        part per perhaps please put pls
        quite
        rather re rather really regarding
        same say see seem seemed seeming seems serious several she should show side since sincere six sixty so some somehow
        someone something sometime sometimes somewhere still such system smh
        take ten than that the their them themselves then thence there thereafter thereby therefore therein thereupon these
        they thick thin third this those though three through throughout thru thus to together too top toward towards twelve
        twenty two tho
        un under until up unless upon us used using
        various very via
        was we well were what whatever when whence whenever where whereafter whereas whereby wherein whereupon wherever
        whether which while whither who whoever whole whom whose why will with within without would
        xxx
        yet yep you your yours yourself yourselves
    """

    return set(w for w in stop_words.split())


def remove_stopwords(tokens):
    """ removes stop words from tokens """
    stop_words = stop_words_list()
    stop_list = set(stopwords.words('english') + stopwords.words('spanish') +
                    stopwords.words('french')) | stop_words

    return [token for token in tokens if token not in stop_list]


def preprocess_text(text):
    filters = [lambda x: x.lower(), strip_urls, strip_punctuation,
               strip_numerics, strip_short, tokenize, remove_stopwords]

    for f in filters:
        text = f(text)

    return text


"""
----------------------------------------------------------------------------------
                        Preparation for Topic Modelling
----------------------------------------------------------------------------------
"""


def def_language(col):
    cursor = col.find(filter=None,
                      projection={'_id': True, 'lang_id': True, 'text': True})

    for doc in cursor:
        lang_id = doc.get('lang_id', None)

        if lang_id is None:
            lang_id = guess_language(doc['text'])

        if lang_id is 'UNKNOWN':
            lang_id = 'und'

        col.update_one({'_id': doc['_id']},
                       {'$set': {'lang_id': lang_id}})


def tokenize_documents(col, documents):
    # cursor = col.find(filter={'lang_id': 'en', 'tokenized': False}, projection={'_id': True, 'text': True})

    for doc in documents:
        doc['tokens'] = preprocess_text(doc['text'])

        col.update_one({'_id': doc['_id']},
                       {'$set': {'tokens': doc['tokens']}})


def make_dictionary(col, params):
    token_dict = corpora.Dictionary()
    cursor = col.find(filter={'lang_id': 'en'}, projection={'tokens': True, 'lang_id': True})

    for doc in cursor:
        token_dict.doc2bow(doc['tokens'], allow_update=True)

    token_dict.filter_extremes(no_below=5, no_above=0.70, keep_n=None)
    token_dict.compactify()

    cursor.rewind()
    token_corpus = [token_dict.doc2bow(doc['tokens']) for doc in cursor]

    token_dict.save(params['dict_file'])
    corpora.MmCorpus.serialize(params['corpus_file'], token_corpus)


if __name__ == '__main__':
    params = ut.get_params(mongo=True, lda=True)

    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',
                        level=logging.INFO)

    overall_time = time()-st_time
    ut.secs_to_hrs(overall_time, 'Overall process took:')



