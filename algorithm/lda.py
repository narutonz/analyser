import logging
from analyser.extras import utils
from gensim import corpora, models
from time import time, strftime, gmtime


if __name__ == '__main__':

    # get parameters from config file and set logging
    lda_params = utils.get_params(lda=True)
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',
                        level=logging.INFO)

    # load the corpus and dictionary
    corpus = corpora.MmCorpus(lda_params['corpus_file'])
    dictionary = corpora.Dictionary.load(lda_params['dict_file'])

    print("Starting at {}".format(strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())))
    start_time = time()
    print("Running LDA with: %s  " % lda_params)

    lda = models.LdaMulticore(corpus=corpus, num_topics=lda_params['num_topics'],
                              id2word=dictionary, workers=lda_params['workers'],
                              chunksize=lda_params['chunks'], passes=lda_params['passes'],
                              alpha=lda_params['alpha'],)

    print("Ended at {}".format(strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())))
    utils.secs_to_hrs(time()-start_time, 'Overall process took: ')

    lda.save(lda_params['lda_file'])
    print("lda saved in %s " % lda_params['lda_file'])
