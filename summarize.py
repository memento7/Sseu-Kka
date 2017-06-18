# -*- coding: utf-8 -*-
from itertools import combinations
from collections import Counter

from konlpy.tag import Kkma
import networkx

STOPWORDS_FILE = './data/stopwords.txt'
import codecs
def get_stopwords():
    return codecs.open(STOPWORDS_FILE,encoding='UTF-8').read().split('\n')

def occurrence(first, second):
    p = sum((first['counter'] & second['counter']).values())
    q = sum((first['counter'] | second['counter']).values())
    return p / q if q else 0

def build_graph(sentences):
    graph = networkx.Graph()
    graph.add_nodes_from(sentences)
    for first, second in combinations(sentences, 2):
        weight = occurrence(first[1], second[1])
        if weight:
            graph.add_edge(first[0], second[0], weight=weight)
    return graph

STOPWORDS = get_stopwords()
TAGGER = Kkma()
def get_summarize(text, count=3):
    sentences = [(num, {
        'text': line + '.',
        'counter': Counter(filter(lambda x: x not in STOPWORDS, TAGGER.nouns(line)))
    }) for num, line in enumerate(text.split('. '))]
    pagerank = networkx.pagerank(build_graph(sentences), weight='weight')
    reordered = sorted(pagerank, key=pagerank.get, reverse=True)
    for index in reordered[:count]:
        yield sentences[index][1]['text']
        