from os import environ
from time import sleep
from typing import Union

import memento_settings as MS

from elasticsearch import Elasticsearch
import requests

ES = Elasticsearch(**MS.SERVER_ES_INFO)

def make_clear(results: list,
               key_lambda=lambda x: x['_id'],
               value_lambda=lambda x: x,
               filter_lambda=lambda x: x) -> dict:
    def clear(result) -> dict:
        result['_source']['_id'] = result['_id']
        return result['_source']
    iterable = filter(filter_lambda, map(clear, results))
    return {key_lambda(source): value_lambda(source) for source in iterable}

def get_scroll(query={}, doc_type='', index='information'):
    array = []
    def _get_scroll(scroll) -> Union[int, list]:
        doc = scroll['hits']['hits']
        array.extend(doc)
        return True if doc else False
    scroll = ES.search(index=index, doc_type=doc_type, body=query, scroll='1m', size=1000)
    scroll_id = scroll['_scroll_id']
    while _get_scroll(scroll):
        scroll = ES.scroll(scroll_id=scroll_id, scroll='1m')
    return make_clear(array)

def put_item(item: dict, doc_type: str, index: str):
    result = ES.index(
        index=index,
        doc_type=doc_type,
        body=item
    )
    print(index, doc_type, result['_id'])
    return result['_id']

def get_clusters() -> dict:
    return get_scroll({}, index='cluster')

def get_entities() -> dict:
    return get_scroll({}, 'namugrim')

def put_event(host=MS.SERVER_API+'', payload={}, headers={
        "Content-Type" : "application/json",
        "charset": "utf-8",
        "Authorization": environ['MEMENTO_BASIC']
    }):
    print (payload)
    # while True:
    #     try:
    #         req = requests.post(host, json=payload, headers=headers)
    #         return req.text
    #     except requests.exceptions.ConnectionError:
    #         sleep(3)
    #         continue
