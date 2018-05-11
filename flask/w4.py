
import json
from elasticsearch import Elasticsearch

from flask import Flask, jsonify, render_template
app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

@app.route("/")
def hello():
    es = Elasticsearch(
            ['ip-10-0-0-8:9200', 'ip-10-0-0-10:9200', 'ip-10-0-0-6', 'ip-10-0-0-12:9200'],
            http_auth=(env['ES_USER'], env['ES_PASS'])
            )
    doc = {
            'size' : 10000,
            'query': {
                'match' : {'fileName': '10'}
                }
            }

    res = es.search(index="books", doc_type='sentences', body=doc, scroll='1m')
    books = res.get('hits').get('hits')
    toweb = []
    for book in books:
        book = book.get('_source')#.get('sentence')
        toweb.append(book)
        # book = json.dumps(book)
        # toweb = toweb + book + "\n"
    # return toweb
    return jsonify(toweb)

@app.route('/welcome')
def welcome():
    es = Elasticsearch(
            ['ip-10-0-0-8:9200', 'ip-10-0-0-10:9200', 'ip-10-0-0-6', 'ip-10-0-0-12:9200'],
            http_auth=(env['ES_USER'], env['ES_PASS'])
            )
    doc = {
            'size' : 10000,
            'query': {
                'match_all' : {}
                }
            }

    res = es.search(index="books", doc_type='sentences', body=doc,scroll='1m')
    books = res.get('hits').get('hits')
    print(type(books))
    return render_template('welcome.html', books=books, my_string="Wheeeee!", my_list=[0,1,2,3,4,5])

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
