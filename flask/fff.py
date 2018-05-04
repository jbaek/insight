
import json
from elasticsearch import Elasticsearch

from flask import Flask
app = Flask(__name__)

@app.route("/")
def hello():
    es = Elasticsearch(['localhost:9200/'])
    doc = {
            'size' : 10000,
            'query': {
                'match_all' : {}
                }
            }

    res = es.search(index="sentences", doc_type='testdoctype', body=doc,scroll='1m')
    # books = res.get('hits').get('hits')
    # for book in books:
        # book = book.get('_source').get('sentence')
    return json.dumps(res, indent=4)


if __name__ == '__main__':
    app.run(host='0.0.0.0')
