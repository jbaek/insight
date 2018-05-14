import json
from os import environ as env

from elasticsearch import Elasticsearch

INDEX = 'books'
sample_size = 100

es = Elasticsearch(
        ['ip-10-0-0-8:9200', 'ip-10-0-0-10:9200', 'ip-10-0-0-6', 'ip-10-0-0-12:9200'],
        http_auth=(env['ES_USER'], env['ES_PASS'])
        )
doc_all = {
        'size' : 10000,
        'query': {
            'match_all' : {}
            }
        }
doc_buckets = {
        "aggs" : {
            "books" : {
                "terms" : { "field" : "fileName.keyword" }
                }
            }
        }

doc_sum = {
        'size' : 10000,
        'query': {
            'match_all' : {}
            },
        "aggs" : {
            "sum_multiSyllables" : { "sum" : { "field": "multiSyllableCount" } },
            "sentenceCount" : { "value_count" : { "field" : "_id" } }
            }
        }

doc_groupby = {
        "size": 10000,
            "aggs": {
                "group_by_book": {
                    "terms": {
                        "field": "fileName.keyword"
                        },
                    "aggs": {
                        "sum_multiSyllables": {
                            "sum": {
                                "field": "multiSyllableCount"
                                }
                            }
                        }
                    }
                }
            }

res = es.search(index=INDEX, doc_type='sentences', body=doc_groupby, scroll='1m')
# res = es.search(index="sentences", doc_type='testdoctype', body=doc,scroll='1m')
with open('from_es.txt', 'w') as the_file:
    the_file.write(json.dumps(res, indent=4))

books = res.get('hits').get('hits')
for book in books[:sample_size]:
    book = book.get('_source') #.get('sentence')
    print(json.dumps(book, indent=4))

