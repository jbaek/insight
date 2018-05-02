
from elasticsearch import Elasticsearch

es = Elasticsearch(['localhost:9200/'])
doc = {
        'size' : 10000,
        'query': {
            'match_all' : {}
            }
        }

res = es.search(index="sentences", doc_type='testdoc', body=doc,scroll='1m')
print(res)
