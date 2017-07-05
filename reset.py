from elasticsearch import Elasticsearch
from os import environ
es = Elasticsearch(host='server2.memento.live', 
                   http_auth=(environ['MEMENTO_ELASTIC'], environ['MEMENTO_ELASTIC_PASS']))

def make_clear(results: list,
               key_lambda = lambda x: x['_id'],
               value_lambda = lambda x: x,
               filter_lambda = lambda x: x) -> dict:
    def clear(result) -> dict:
        result['_source']['_id'] = result['_id']
        return result['_source']
    iterable = filter(filter_lambda, map(clear, results))
    return {key_lambda(source): value_lambda(source) for source in iterable}


# In[4]:

def get_scroll(body={}, doc_type='', index='memento', time='1m'):
    array = []
    def _get_scroll(scroll):
        doc = scroll['hits']['hits']
        array.extend(doc)
        return doc and True or False
    scroll = es.search(index=index, doc_type=doc_type, body=body, scroll=time, size=1000)
    scroll_id = scroll['_scroll_id']
    while _get_scroll(scroll):
        scroll = es.scroll(scroll_id=scroll_id, scroll=time)
    return make_clear(array)

raw_clusters = get_scroll(index='memento',doc_type='cluster',body={
    "query": {
        "match": {
            "task": "doing"
        }
    }
})

for cluster in raw_clusters.values():
    es.update(index='memento',doc_type='cluster',id=cluster['_id'],body={
        "script" : "ctx._source.remove(\"task\")"
    })
    