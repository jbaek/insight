
import json
from os import environ as env
from elasticsearch import Elasticsearch

from flask import Flask, jsonify, url_for, render_template, request
app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

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

@app.route('/search/score')
def search_score():
    return render_template('search-score.html')

@app.route('/search/score/results', methods=['GET', 'POST'])
def search_score_request():
    range_start = request.form["range-start"]
    range_end = request.form["range-end"]
    doc = {
            'size' : 10000,
            'query': {
                'match_all' : {}
                }
            }
    res = es.search (
        index="books",
        size=20,
        body=doc
    )

    books = res.get('hits').get('hits')
    for book in books:
        book = book.get('_source')

    return render_template('results.html', res=res )

@app.route('/search/phrase')
def search_phrase():
    return render_template('search-phrase.html')

@app.route('/search/phrase/results', methods=['GET', 'POST'])
def search_phrase_request():
    search_term = request.form["input"]
    res = es.search(
        index="books",
        size=20,
        body={
            "query": {
                "multi_match" : {
                    "query": search_term,
                    "fields": [
                        "sentenceText",
                    ]
                }
            }
        }
    )
    return render_template('results.html', res=res )

@app.route("/hello")
def hello():

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

    res = es.search(index="books", doc_type='sentences', body=doc,scroll='1m')
    books = res.get('hits').get('hits')
    print(type(books))
    return render_template('welcome.html', books=books, my_string="Wheeeee!", my_list=[0,1,2,3,4,5])

@app.route('/info')
def api_info():
    return jsonify(es.info())

@app.route('/health')
def api_health():
    return jsonify(es.cluster.health())


@app.route('/articles')
def api_articles():
    return 'List of ' + url_for('api_articles')


@app.route('/articles/<articleid>')
def api_article(articleid):
    return 'You are reading ' + articleid


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
