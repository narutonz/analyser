import random
import analyser.extras.utils as utils
from collections import defaultdict, Counter
from gensim import models, corpora
from sklearn.metrics.pairwise import cosine_similarity as sklearn_cosine


def randomize_data(cursor_object, sample_size):
    data = []
    rand_index = random.sample(range(cursor_object.count()), sample_size)
    rand_index.sort()

    if cursor_object.count() > sample_size:
        for i in range(cursor_object.count()):
            if i in rand_index:
                data.append(cursor_object[i])
    else:
        data = list(cursor_object)

    return data


class ScoreCalculator(object):
    def __init__(self, current_user, closest_neighbour, mongo_instance, n=10):
        self._mongo = mongo_instance
        self.active = current_user
        self.closest_neighbour = closest_neighbour
        self.repeat_n = n

        self.a = []
        self.b = []

        self.score_topic = []
        self.score_source = []
        self.score_location = []
        self.score_interactions = []
        self.score_vocabulary = []
        self.total_similarity = []

        self.tokens_a = []
        self.tokens_b = []

        self.token_dict = corpora.Dictionary.load('outputs/default.dict')
        self.lda = models.LdaMulticore.load('outputs/default.lda')

    def single_user(self):
        """ TEST: to check the algorithm use it on the same account and split tweets in to 2 random halves """
        cursor = self._mongo.db['user_tweets'].find({'user_id_str': self.active['user_id_str']})

        sample_size = cursor.count() // 2
        rand_index = random.sample(range(cursor.count()), sample_size)
        rand_index.sort()

        for i in range(cursor.count()):
            if i in rand_index and len(self.a) != sample_size:
                self.a.append(cursor[i])
            elif i not in rand_index and len(self.b) != sample_size:
                self.b.append(cursor[i])
            else:
                continue

    def multiple_users(self, current_neighbour):
        """
            - initialises the data sets for the user and the current nearest neighbour
            - data sample size is equivalent to the lowest of the two
            - in the event the data size in the database is larger than sample size,
                a random sample is taken
        """
        sample_size = min(current_neighbour['tweet_count'], self.active['tweet_count'])

        if sample_size != 0:
            cursor = self._mongo.db['user_tweets'].find({'user_str_id': self.active['user_str_id']})
            self.a = randomize_data(cursor, sample_size)

            cursor = self._mongo.db['user_tweets'].find({'user_str_id': current_neighbour['user_str_id']})
            self.b = randomize_data(cursor, sample_size)
        else:
            self.a = None
            self.b = None

    def topic_scoring(self):
        """
            - two topic scores are taken:
                * what the user tweets about the most (all the tokens)
                * what the user talks about in general (a set of the tokens are used [no duplicates])
            - these two cosine similarity results are then weighted (in favour of 1st category) and summed
        """
        for tw_a, tw_b in zip(self.a, self.b):
            self.tokens_a.extend(tw_a['tokens'])
            self.tokens_b.extend(tw_b['tokens'])

        topics_a = [self.lda[self.token_dict.doc2bow(self.tokens_a)],
                    self.lda[self.token_dict.doc2bow(set(self.tokens_a))]]
        topics_a = [utils.normalize_counts(dict(topics_a[0])),
                    utils.normalize_counts(dict(topics_a[1]))]

        topics_b = [self.lda[self.token_dict.doc2bow(self.tokens_b)],
                    self.lda[self.token_dict.doc2bow(set(self.tokens_b))]]
        topics_b = [utils.normalize_counts(dict(topics_b[0])),
                    utils.normalize_counts(dict(topics_b[1]))]

        return (utils.cosine_similarity(topics_a[0], topics_b[0]) * 0.9 +
                utils.cosine_similarity(topics_a[1], topics_b[1]) * 0.1)

    def source_scoring(self):
        """ scores the source of the tweets (iPhone, Android, Web etc) """
        sources_a = defaultdict(int)
        sources_b = defaultdict(int)

        for tw_A in self.a:
            sources_a[tw_A['source']] += 1

        for tw_B in self.b:
            sources_b[tw_B['source']] += 1

        sources_a = utils.normalize_counts(sources_a)
        sources_b = utils.normalize_counts(sources_b)

        return utils.cosine_similarity(sources_a, sources_b)

    def location_scoring(self):
        """ finds the similarity between the general location of tweets """
        locations_a = defaultdict(int)
        locations_b = defaultdict(int)

        for tw_A in self.a:
            try:
                locations_a[tw_A['place']['name']] += 1
            except TypeError:
                locations_a['none'] += 1
                pass

        for tw_B in self.b:
            try:
                locations_b[tw_B['place']['name']] += 1
            except TypeError:
                locations_b['none'] += 1
                pass

        locations_a = utils.normalize_counts(locations_a)
        locations_b = utils.normalize_counts(locations_b)

        return utils.cosine_similarity(locations_a, locations_b)

    def interactions_scoring(self):
        """ finds the similarity between the interactions between the two users """
        mentions_a = defaultdict(int)
        mentions_b = defaultdict(int)

        for tw in self.a:
            for name in tw['mentions']['screen_names']:
                mentions_a[name] += 1

        for tw in self.b:
            for name in tw['mentions']['screen_names']:
                mentions_b[name] += 1

        mentions_a = utils.normalize_counts(mentions_a)
        mentions_b = utils.normalize_counts(mentions_b)

        return utils.cosine_similarity(mentions_a, mentions_b)

    def vocabulary_scoring(self):
        # try:
        #     variety_a = len(set(self.tokens_a)) / len(self.tokens_a)
        # except ZeroDivisionError:
        #     variety_a = 0
        # try:
        #     variety_b = len(set(self.tokens_b)) / len(self.tokens_b)
        # except ZeroDivisionError:
        #     variety_b = 0
        #
        # variety_score = sklearn_cosine(variety_a, variety_b)[0]

        counter_a = Counter(self.tokens_a)
        counter_a = utils.normalize_counts(counter_a)
        counter_b = Counter(self.tokens_b)
        counter_b = utils.normalize_counts(counter_b)

        word_count_score = utils.cosine_similarity(counter_a, counter_b)
        # bigram and trigram scoring

        # return variety_score * 0.5 + word_count_score * 0.5
        return word_count_score

    def overall_similarity(self):
        score_lists = [self.score_topic, self.score_interactions, self.score_location,
                       self.score_source, self.score_vocabulary]
        weights = [0.35, 0.3, 0.25, 0.05, 0.05]
        rescaled_weights = [0.5, 0, 0.36, 0.07, 0.07]

        for i in range(len(self.score_topic)):
            total = 0
            for j in range(len(weights)):
                if self.score_interactions[i] != 0.0:
                    total += score_lists[j][i] * weights[j]
                else:
                    total += score_lists[j][i] * rescaled_weights[j]

            self.total_similarity.append(total)

    def average(self, score_list):
        for k in score_list.keys():
            score_list[k] /= self.repeat_n

        return score_list

    def reset(self):
        self.a = []
        self.b = []

        self.tokens_a = []
        self.tokens_b = []

    def calculate_scores(self, score_list):
        score_names = ['topic', 'interactions', 'location',
                       'vocabulary', 'source']
        scoring_funcs = [self.topic_scoring, self.interactions_scoring, self.location_scoring,
                         self.source_scoring, self.vocabulary_scoring]
        if self.a is not None and self.b is not None:
            for key, func in zip(score_names, scoring_funcs):
                score_list[key] += (func())
        else:
            for key in score_names:
                score_list[key] = 0.0

        return score_list

    def process(self):
        final_scores = defaultdict(float)
        average_score_list = [self.score_topic, self.score_interactions, self.score_location,
                              self.score_source, self.score_vocabulary]
        if self.closest_neighbour is None:
            for i in range(self.repeat_n):
                self.single_user()
                final_scores = self.calculate_scores(final_scores)
                self.reset()

            final_scores = self.average(final_scores)
            for value, score in zip(final_scores.values(), average_score_list):
                score.append(value)
            self.overall_similarity()
        else:
            for neighbour in self.closest_neighbour:
                self.multiple_users(neighbour)
                final_scores = self.calculate_scores()

                for value, score in zip(final_scores.values(), average_score_list):
                    score.append(value)
            self.overall_similarity()

    def __str__(self):
        return '\tResults Matrix for {}\n\n)'

if __name__ == '__main__':
    mongo = utils.Database('final_db')
    random.seed(None)
