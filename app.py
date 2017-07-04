
# coding: utf-8

# In[2]:

from elasticsearch import Elasticsearch
from os import environ
es = Elasticsearch(host='server2.memento.live', 
                   http_auth=(environ['MEMENTO_ELASTIC'], environ['MEMENTO_ELASTIC_PASS']))


# In[3]:

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


# In[155]:

SERVER_API = 'https://manage.memento.live/api/'
HEADERS = {"Content-Type" : "application/json",
           "charset": "utf-8",
           "Authorization": environ['MEMENTO_BASIC']}


# In[338]:

import requests
from time import sleep
import json
import re


# In[159]:

def request_post(api, payload={}):
    print ('POST request', api)
    while True:
        try:
            res = requests.post(SERVER_API + api, headers=HEADERS, verify=True, json=payload)
            break
        except:
            print ('connection error, wait 2s')
            sleep(2)
    response = res.text
    print(response)
    if response:
        return json.loads(response)
    return True

def request_get(api, payload={}):
    print ('GET request', api)
    while True:
        try:
            res = requests.get(SERVER_API + api, headers=HEADERS, verify=True, json=payload)
            break
        except:
            print ('connection error, wait 2s')
            sleep(2)
    return json.loads(res.text)


# In[307]:

def get_entity_tag(entity):
    doc = es.get(index='memento',doc_type='entities',id=entity)['_source']
    if 'flag' not in doc:
        return []
    result = es.get(index='memento', doc_type='namugrim', id=doc['flag'])['_source']
    return result['tags']
    
def get_entity_info(entity):
    doc = es.get(index='memento',doc_type='entities',id=entity)['_source']
    result = es.get(index='memento', doc_type='namugrim', id=doc['flag'])['_source']
    return result['keyword'], result['nickname'], result['realname'], result['subkey']

def func_call():
    pass

    slov = ();


role_dict = {
    '배우': 'ACTOR',
    '가수': 'SINGER',
    '정치인': 'POLITICIAN',
    '스포츠선수': 'SPORTS',
    '그룹가수': 'BAND',
    '모델': 'MODEL',
    '코미디언': 'COMEDIAN',
    '기업인': 'ENTREPRENEUR',
    '공인': 'PUBLIC_FIGURE',
}
def put_new_entity(entity):
    doc = es.get(index='memento',doc_type='entities',id=entity)['_source']
    namugrim = es.get(index='memento',doc_type='namugrim',id=doc['flag'])['_source']
    payload = {
        "nickname": namugrim['nickname'],
        "realname": namugrim['realname'],
        "role_json": {'PERSON':[]},
        "status": "SHOW",
        "subkey": namugrim['subkey']
    }
    for role in doc['subkey'] + [namugrim['subkey']]:
        if role in role_dict:
            payload['role_json'][role_dict[role]] = []
    res = request_post('persist/entities', payload)
    es.update(index='memento',doc_type='entities',id=entity,body={
        'doc': {
            'eid': res['id']
        }
    })
    return res


# In[5]:

from itertools import chain
from collections import defaultdict


# In[6]:

raw_clusters = get_scroll(index='memento',doc_type='cluster',body={
    "query": {
        "bool": {
            "mustNot": [{         
                "exists" : {
                    "field" : "task"
                }
            }]
        }
    }
})


# In[120]:

clusters = sorted(raw_clusters.values(), key=lambda x: x['topic']['published_time'])


# In[121]:
for cluster in clusters:
    es.update(index='memento', doc_type='cluster', id=cluster['_id'], body={
        'doc': {
            'task': 'doing'
        }
    })

clusters = list(filter(lambda x: x['accuracy'] < 1.5, clusters))


# In[ ]:

for idx, cluster in enumerate(clusters):
    if len(cluster['entity']):
        clusters[idx]['entity'] = list(filter(lambda x: len(x) > 1, cluster['entity']))


# In[11]:

if not len(raw_clusters):
    print('there all done!')
    exit()
print ("accuracy: {}\nclusters: {}".format(len(clusters)/len(raw_clusters), len(clusters)))


# In[68]:

duplication = defaultdict(list)
total_duplication = set()
for idx, cluster in enumerate(clusters):
    if idx in total_duplication: continue
    for jdx, dluster in enumerate(clusters[max(idx-1000, 0):min(idx+1000, len(clusters))]):
        jdx += max(idx-1000, 0)
        if jdx in duplication or idx == jdx: continue
        if cluster['topic']['title'] == dluster['topic']['title']:
            duplication[idx].append(jdx)
            total_duplication.add(jdx)


# In[74]:

print ("result unique cluster: {}\ntotal duplication: {}".format(len(clusters) - len(total_duplication), len(total_duplication)))


# In[128]:

for tar, duplist in duplication.items():
    items = set(clusters[tar]['items'])
    rel_entities = clusters[tar]['topic']['entities']
    if isinstance(rel_entities, str):
        rel_entities = rel_entities.split(' ')
    entities = set(clusters[tar]['entity'] + rel_entities)
    entity = set(clusters[tar]['entity'])
    for dup in duplist:
        items.update(set(clusters[dup]['items']))
        rel_entities = clusters[dup]['topic']['entities']
        if isinstance(rel_entities, str):
            entities.update(set(clusters[dup]['entity'] + [rel_entities]))
        entity.update(clusters[dup]['entity'])
    clusters[tar]['items'] = list(items)
    clusters[tar]['entity'] = list(entity)
    clusters[tar]['topic']['entities'] = list(entities)


# In[129]:

for idx, cluster in enumerate(clusters):
    if isinstance(cluster['topic']['entities'], str):
        clusters[idx]['topic']['entities'] = cluster['topic']['entities'].split(' ')
    if isinstance(cluster['entity'], str):
        clusters[idx]['entity'] = [cluster['entity']]


# In[131]:

all_entities = set()
all_entity = set()
for cluster in clusters:
    all_entities.update(set(cluster['topic']['entities']))
    all_entity.update(set(cluster['entity']))


# In[147]:

uniq = list(range(len(clusters)))
for dup in total_duplication:
    uniq.remove(dup)


# # entity add

# In[305]:

entities_eid = get_scroll(index='memento',doc_type='entities',body={
    "_source": ['eid'],
    "query": {
        "exists" : { "field" : "eid" }
    }
})


# In[195]:

for entity in all_entity:
    if entity not in entities_eid:
        res = put_new_entity(entity)
        entities_eid[entity] = res['id']


# In[226]:

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


# In[234]:

def get_similar(item, entity):
    info = " ".join(list(map(lambda x: x['tag'], get_entity_tag(entity)))[:128])
    tfidf_vectorizer = TfidfVectorizer()
    tfidf_matrix_train = tfidf_vectorizer.fit_transform([item, info])
    return cosine_similarity(tfidf_matrix_train[0:1], tfidf_matrix_train)[0][1]


# In[247]:

for idx in uniq:
    if not idx % 5000: print ("{}/{}".format(idx, len(clusters)))
    topic = clusters[idx]['topic']
    clusters[idx]['topic']['entities'] = [entity for entity in 
            [entity for entity in topic['entities'] if entity in 
             entities_eid] if get_similar(topic['content'], entity) > 0.025]


# In[256]:

from konlpy.tag import Komoran
tagger = Komoran()


# In[421]:

news_pubs = {'모바일','경악', '화제', '포토', '네이버', '뉴스', '종합', '경향신문', '국민일보', '내일신문', '동아일보', '문화일보', '서울신문', '세계일보', '조선일보', '중앙일보', '한겨레', '한국일보', '방송', '통신', '뉴스1', '뉴시스', '연합뉴스', '연합뉴스TV', '채널A 한국경제TV', '한국경제TV', 'JTBC', 'KBS', '뉴스', 'MBC', 'MBN', 'SBS', 'CNBC', 'SBS', 'TV조선', 'YTN', '경제', '매일경제', '머니투데이', '서울경제', '아시아경제', '이데일리', '조선비즈', '파이낸셜뉴스', '한국경제', '헤럴드경제', 'SBS CNBC', '인터넷', '노컷뉴스', '데일리안', '미디어오늘', '오마이뉴스', '쿠키뉴스', '프레시안', 'IT', '디지털데일리', '디지털타임스', '블로터 아이뉴스24', '전자신문', 'ZDNet Korea'}
keyword_ban = {'금지','재배포','무단','배포','바로가기', '페이스북','트위터','전재', '=', '무단전재', '기자'}
keyword_ban.update(news_pubs)


# In[422]:

def keywords_filter(keywords):
    getnouns = [(
        "".join([tag[0] for tag in tagger.pos(key['keyword']) if tag[1].startswith('NN')]),
        key['value']
    ) for key in keywords]
    keywords = defaultdict(int)
    for keyword in getnouns:
        keywords[keyword[0]] += keyword[1]
    return [{
        'keyword': keyword[0],
        'value': keyword[1]
    }for keyword in keywords.items() if keyword[0] and keyword[0] not in keyword_ban]


# In[484]:

def title_filter(title):
    def get_readable(text):
        sub_pattern = ['\(.+?\)', '\[.+?\]', '\{.+?\}', '<.+?>', '~~(.+?)~~']
        del_pattern = ['\'', '\"', '’', '‘', ':', '“']
        for pattern in sub_pattern:
            text = re.sub(pattern, '', text)
        return [word for word in text.translate(str.maketrans({c:'' for c in del_pattern})).strip().split(' ') if word]
    return " ".join([word for word in get_readable(title) if word not in news_pubs])

# In[477]:

def push_article(eid, title, href, comment_count, imgs):
    payload = {
        "comment_count": comment_count,
        "crawl_target": "네이버 뉴스",
        "source_url": href,
        "summary": "",
        "title": title_filter(title)
    }
    if imgs:
        payload['image'] = {
            "url": imgs[0],
            "source_link": href,
            "weight": 5
        }
    return request_post('persist/events/{}/articles'.format(str(eid)), payload)

def push_event(topic, rate, keywords, emotions):
    return request_post('persist/events', {
        "date" : (str(topic['published_time'])[:10] + " 00:00:00").replace('.', '-'),
        "title" : title_filter(topic['title']),
        "type" : topic['cate'],
        "status" : 0,
        "issue_score" : rate,
        "emotions" : [{
            "emotion": emotion_info['emotion'],
            "weight": emotion_info['value']
        } for emotion_info in emotions],
        "images": [{
            "url": img,
            "source_url": topic['href_naver'],
            "type": "",
            "weight": 10
        } for img in topic['imgs']],
        "keywords" : [{
            "keyword": keyword_info['keyword'],
            "weight": keyword_info['value'] * 100
        } for keyword_info in keywords_filter(keywords)],
        "summaries" : []
    })


# In[489]:

import pymysql
def connection(host='server1.memento.live',
               user='memento',
               password=environ['MEMENTO_PASS'],
               db='memento',
               charset='utf8mb4'):
    conn = pymysql.connect(host=host,
                           user=user,
                           password=password,
                           db=db,
                           charset=charset,
                           cursorclass=pymysql.cursors.SSCursor)
    cur = conn.cursor()
    return conn, cur

def disconnect(conn, cur):
    cur.close()
    conn.close()
conn, cur = connection()

from summarize import get_summarize
query = '''INSERT INTO event_summaries_3line (event_id, user_id, summaries_3line) VALUES ({}, 1, '[\"{}\", \"{}\", \"{}\"]');'''
def push_summarize(content, event_id):
    summarize = list(map(title_filter, get_summarize(content)))
    while len(summarize) < 3:
        summarize.append("")
    a,b,c = summarize
    while True:
        try:
            result = cur.execute(query.format(event_id, a, b, c))
            break
        except:
            print (result)
            print('summarize:',query.format(event_id, a, b, c))
            print('summarize push error, wait 2s')
            sleep(2)
            continue
    conn.commit()

print ('start push!!')
for idx in uniq:
    if not idx % 5000: print ("{}/{}".format(idx, len(clusters)))
    cluster = clusters[idx]
    res = push_event(cluster['topic'], cluster['rate'], cluster['keywords'], cluster['emot'])
    try:
        event_id = res['id']
    except:
        print (idx, res)
        continue
    images = []
    for article in [es.get(index='memento',doc_type='News_Naver',id=item)['_source'] for item in cluster['items']]:
        push_article(event_id, article['title'], article['href_naver'], article['reply_count'], article['imgs'])
        for image in article['imgs']:
            images.append((article['href_naver'], image))
        
    request_post('persist/events/{}/images'.format(event_id), [{
        "like_count": 0,
        "source_link": source,
        "type": "string",
        "url": url,
        "weight": 0
    } for source, url in images])

    for entity in set(cluster['topic']['entities']):
        if entity in entities_eid:
            entity_id = entities_eid[entity]['eid']
            request_post('persist/entities/{}/events/{}'.format(str(entity_id), str(event_id)))
    push_summarize(cluster['topic']['content'], event_id)
conn.commit()
disconnect(conn, cur)

for cluster in clusters:
    es.update(index='memento', doc_type='cluster', id=cluster['_id'], body={
        'doc': {
            'task': 'done'
        }
    })
# In[ ]:



