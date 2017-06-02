import numpy as np
from collections import defaultdict
from connection import get_clusters, get_entities, put_event
from utility import get_property, extract_entities, get_similar
import pickle

def process():
    keywords = defaultdict(lambda : defaultdict(list))
    clusters = list(get_clusters().values())
    entities = get_entities()

    print (len(clusters))
    for idx, cluster in enumerate(clusters):
        print (idx)
        keyword = cluster['topic']['keyword']
        clusters[idx]['keyword'] = [keyword]
        context = [str(item) for item in cluster['items']]
        
        isEntity = lambda x: x in entities
        isRelated = lambda x: np.mean(get_similar(x, context, entities)) > 0.025 and x != cluster['topic']['keyword']

        ne = [entity for entity in extract_entities(" ".join(context)) if isEntity(entity)]

        cluster['related'] = {entity for entity in ne if isRelated(entity)}

        for rel in cluster['related']:
            keywords[keyword][rel].append(idx)

    pickle.dumps(keywords, open('keywords.p', 'wb'))
    pickle.dumps(clusters, open('clusters.p', 'wb'))

if __name__ == '__main__':
    process()