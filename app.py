from collections import defaultdict

import numpy as np

from utility import Logging
from connection import get_clusters
from utility import get_property, extract_entities, get_similar, ENTITIES

MINIMUM_RELATION = 3

def process():
    keywords = defaultdict(lambda : defaultdict(list))
    clusters = get_clusters()
    for idx, cluster in enumerate(clusters):

        keyword = cluster['keyword']
        context = [str(item) for item in cluster['items']]
        
        isEntity = lambda x: x in ENTITIES
        isRelated = lambda x: np.mean(get_similar(x, context)) > 0.025 and x != cluster['keyword']

        ne = [entity for entity in extract_entities(" ".join(context)) if isEntity(entity)]
        cluster['related'] = {entity for entity in ne if isRelated(entity)}

        for rel in cluster['related']:
            keywords[keyword][rel].append(idx)

    def date_match(idx, jdx):
        return clusters[idx]['topic'].published_time == clusters[jdx]['topic'].published_time

    rcluster = []
    for keyword, relation in keywords.items():
        for entity, rel in relation.items():
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
        pass
        # new cluster, have entities(related) - cluster[c_indexs]

    # todo: process after new-cluster(old)

if __name__ == '__main__':
    process()