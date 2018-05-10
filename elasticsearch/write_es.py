
import json
from os import environ as env

from elasticsearch import Elasticsearch

es = Elasticsearch(['ip-10-0-0-8:9200/'],
        http_auth=(env['ES_USER'], env['ES_PASS'])
        )
doc = {
        'size' : 10000,
        'query': {
            'match_all' : {}
            }
        }

res = es.search(index="books", doc_type='sentences', body=doc,scroll='1m')
# res = es.search(index="sentences", doc_type='testdoctype', body=doc,scroll='1m')
with open('from_es.txt', 'w') as the_file:
    the_file.write(json.dumps(res, indent=4))

books = res.get('hits').get('hits')
for book in books:
    book = book.get('_source') #.get('sentence')
    print(json.dumps(book, indent=4))


def _start_es():
    es=Elasticsearch(
            'ip-10-0-0-8:9200',
            http_auth=(env['ES_USER'], env['ES_PASS'])
            )
    if es.indices.exists('books'):
        es.indices.delete('books')
        es.indices.create('books')

_start_es()
