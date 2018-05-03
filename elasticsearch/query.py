import json
from elasticsearch import Elasticsearch

es = Elasticsearch(['localhost:9200/'])
doc = {
        'size' : 10000,
        'query': {
            'match_all' : {}
            }
        }

res = es.search(index="sentences", doc_type='testdoc', body=doc,scroll='1m')

books = res.get('hits').get('hits')
for book in books:
    book = book.get('_source').get('sentence')
    print(len(book))

with open('from_es.txt', 'w') as the_file:
    the_file.write(json.dumps(res, indent=4))
