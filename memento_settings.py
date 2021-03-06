# Custom Settings
from os import environ

SERVER_RDB = 'server2.memento.live'
SERVER_RDB_INFO = {
    "host": SERVER_RDB,
    "user": 'memento',
    "passwd": environ['MEMENTO_PASS'],
    "db": 'memento',
    "charset": 'utf8',
    "port": 3306
}
SERVER_ES = 'server2.memento.live'
SERVER_ES_INFO = {
    'host': SERVER_ES,
    'port': 9200,
}
SERVER_PDB = 'http://server1.memento.live'
SERVER_PDB_INFO = {
    "host": SERVER_PDB,
    "user": 'memento',
    "passwd": environ['MEMENTO_PASS'],
    "db": 'memento',
    "charset": 'utf8',
    "port": 3306
}
SERVER_API = 'https://api.memento.live/persist/'
SERVER_API_HEADER = { 
    "Content-Type" : "application/json",
    "charset": "utf-8",
    "Authorization": environ['MEMENTO_BASIC']
}

MINIMUM_ITEMS = 8
MINIMUM_CLUSTER = 4

MINIMUM_SIMILAR = 0.03
