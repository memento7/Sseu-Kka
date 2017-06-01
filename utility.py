import logging
from collections import Counter
from os import name
from time import time

POLYGLOT = name == "POSIX"

if POLYGLOT:
    from polyglot.text import Text
from imp import reload
from konlpy.tag import Komoran
from nltk import word_tokenize, pos_tag, ne_chunk
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

class Logging:
    reload(logging)
    __log = logging
    __log.basicConfig(format='%(levelname)s:%(message)s',
                      filename='cluster.log',
                      level=logging.INFO)
    __logger = {
        'INFO': __log.info,
        'DEBUG': __log.debug,
        'warning': __log.warning
    }
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        Logging.__log.info('Logging: {} with args: {}, kwargs: {}'.format(
            self.func.__name__, args, kwargs,
        ))
        ret = self.func(*args, **kwargs)
        Logging.__log.info('Logging: {} returns {}'.format(
            self.func.__name__, type(ret),
        ))
        return ret

    @staticmethod
    def log(msg, level='INFO'):
        Logging.__logger[level](msg)

def filter_quote(quotes):
    return " ".join(["".join(quote) for quote in quotes])

def get_property(items):
    return " ".join([" ".join([item.title, item.content, filter_quote(item.quotes)]) for item in items])

def get_similar(keyword, items, entities):
    entity_tag = list(map(lambda x: (x['tag'], x['value']),entities[keyword]['tags']))
    entity = " ".join([k for k, v in entity_tag[:100]])
    tfidf_vectorizer = TfidfVectorizer()
    tfidf_matrix_train = tfidf_vectorizer.fit_transform([entity] + items)
    return cosine_similarity(tfidf_matrix_train[0:1], tfidf_matrix_train)[0][1:]

tagger = Komoran()
def extract_entities(text):
    nnps = []

    if POLYGLOT:
        polytext = Text(text)
        for entity in polytext.entities:
            nnps.append(entity[0])
            
    for chunk in ne_chunk(pos_tag(word_tokenize(text))):
        if len(chunk) == 1 and chunk.label() == 'ORGANIZATION':
            nnps.append(chunk.leaves()[0][0])
        elif len(chunk)>1 and str(chunk[1]).startswith('NN'):
            nnps.append(chunk[0])

    counter = Counter(nnps).most_common()
    for nnp, count in counter:
        pos = tagger.pos(nnp)
        if len(pos) == 1 and pos[0][1].startswith('NN'):
            yield pos[0][0]
