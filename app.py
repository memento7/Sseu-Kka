from collections import defaultdict

import numpy as np

from utility import Logging
from connection import get_clusters, get_entities, put_event
from utility import get_property, extract_entities, get_similar

MINIMUM_RELATION = 3

def process():
    keywords = defaultdict(lambda : defaultdict(list))
    clusters = list(get_clusters().values())
    entities = get_entities()

    print (len(clusters.values()))
    for idx, cluster in enumerate(clusters):
        if not idx % 100: print (idx)
        keyword = cluster['topic']['keyword']
        clusters[idx]['keyword'] = [keyword]
        context = [str(item) for item in cluster['items']]
        
        isEntity = lambda x: x in entities
        isRelated = lambda x: np.mean(get_similar(x, context, entities)) > 0.025 and x != cluster['topic']['keyword']

        ne = [entity for entity in extract_entities(" ".join(context)) if isEntity(entity)]

        cluster['related'] = {entity for entity in ne if isRelated(entity)}

        for rel in cluster['related']:
            keywords[keyword][rel].append(idx)

    def date_match(idx, jdx):
        return clusters[idx]['topic'].published_time == clusters[jdx]['topic'].published_time

    print('merge start')

    # post -rel detect

    rcluster = []
    for keyword, relation in keywords.items():
        for entity, rel in relation.items():
            print('2step for', entity)
            if len(rel) > MINIMUM_RELATION:
                rel_entity = [keyword]
                rel_index = []
                if entity in keywords:
                    rel_entity.append(entity)
                    for relidx in rel:
                        if relidx in rel_index: continue

                        for reljdx in rel:
                            if relidx != reljdx and date_match(relidx, reljdx):
                                rel_index.append(reljdx)
                else:
                    pass
                if len(rel_index) > 1:
                    rcluster.append((rel_entity, rel_index))

    for entites, c_indexs in rcluster:
        clusters[c_indexs]['keyword'].extend(entities)

    for cluster in clusters:
        put_event(cluster)

if __name__ == '__main__':
    process()