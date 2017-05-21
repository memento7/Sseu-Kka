from os import environ, listdir
import json

from utility import Logging
import memento_settings as MS

from item import myItem as Item
import pandas as pd
import pickle
import pymysql

def connection(kwargs=MS.SERVER_RDB_INFO):
    conn = pymysql.connect(**kwargs,
                           cursorclass=pymysql.cursors.SSCursor)
    cur = conn.cursor()
    return conn, cur

def disconnect(conn, cur):
    cur.close()
    conn.close()

def get_query_result(query, cur, func=list, debug=False):
    if debug:
        Logging.log('<<Query Executed>>: {}'.format(query))
    result = cur.execute(query)
    if debug:
        Logging.log('<<Query Result>>: {}'.format(result))
    return func(cur)

@Logging
def get_entities():
    conn, cur = connection()

    def get_entity(target, cur=cur):
        def _get_(table, target):
            query = "SELECT * FROM entity_{} WHERE target={}".format(table, target)
            func = lambda c: [tuple(v for k, v in zip(c.description, x) if not k[0] in ['id', 'target', 'flag']) for x in c.fetchall()]
            return get_query_result(query, cur, func)

        return {table: _get_(table, target) for table in ['accent', 'link', 'strike', 'tag']}

    query = "SELECT id, keyword FROM entity"
    func = lambda c: [{k[0]:v for k, v in zip(c.description, x)} for x in c.fetchall()]
    result = get_query_result(query, cur, func, debug=True)

    entities = {entity['keyword']: get_entity(entity['id']) for entity in result}
    disconnect(conn, cur)
    return entities

@Logging
def get_clusters():
    clusters = []
    for file in listdir('./pickle'):
        cluster = pickle.load(open('./pickle/' + file, 'rb'))
        cluster['keyword'] = file.split('-')[0]
        clusters.append(cluster)
    return clusters