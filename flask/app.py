import json
import math
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

@app.route('/')
@app.route('/search/score')
def search_score():
    return render_template('search-score.html')

@app.route('/search/score/results', methods=['GET', 'POST'])
def search_score_request():
    range_start = request.form["range-start"]
    range_end = request.form["range-end"]
    app.logger.info("{0} to {1}".format(range_start, range_end))
    doc_groupby = {
            "size": 0,
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
                            # "scores_range": {
                                # "bucket_selector": {
                                    # "buckets_path": {
                                        # "var1": "sum_multiSyllables"
                                        # },
                                    # "script": "params.var1 >= {0}".format(range_start)
                                    # }
                                # }
                            }
                        }
                    }
                }
    res = es.search (
        index="books",
        size=100,
        body=doc_groupby
    )

    books = res.get('aggregations').get('group_by_book')
    books_display = filter_books_score_range(
            books,
            range_start,
            range_end
            )
    return render_template('results-score.html', res=books_display)


def filter_books_score_range(books, range_start, range_end):
    books_display = []
    for book in books['buckets']:
        multisyllable = book['sum_multiSyllables']['value']
        numsentences = book['doc_count']
        smog = calc_smog(multisyllable, numsentences)
        if smog >= float(range_start) and smog <= float(range_end):
            book['smog'] = smog
            book['sum_other_doc_count'] = books.get("sum_other_doc_count")
            books_display.append(book)
    books_sorted = sorted(books_display, key=lambda k: k['smog'])
    return books_sorted


def calc_smog(multisyllable, numsentences):
    return round(1.0430 * math.sqrt(30 * multisyllable / numsentences) + 3.1291, 2)


@app.route('/search/book', methods=['GET', 'POST'])
def search_book_request():
    book_name = request.form["book-name"]
    res = es.search(
        index="books",
        size=30,
        body={
            "sort":
                    { "position" : {"order" : "asc"}},
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "fileName.keyword": book_name
                                }
                            },
                        {
                            "range": {
                                "numWordsInSentence": {
                                    "gt": "0",
                                    "lt": "10000"
                                    }
                                }
                            }
                        ]
                    }
                },
            "size": 30,
            "aggs": {
                "dedup" : {
                    "terms": {
                        "field": "position",
                        "order" : { "_key" : "asc" },
                        "size": 30
                        },
                    "aggs": {
                        "dedup_docs": {
                            "top_hits": {
                                "size":1
                                }
                            }
                        }
                    }
                }
            }
        )

    # books = res.get('hits').get('hits')
    # books = [book.get('_source') for book in books]
    books = res.get('aggregations').get('dedup').get('buckets')
    books = [book.get('dedup_docs').get('hits').get('hits')[0].get('_source') for book in books]

    return render_template('results-score-book.html', books=books)

@app.route('/search/phrase')
def search_phrase():
    return render_template('search-phrase.html')

@app.route('/search/phrase/results', methods=['GET', 'POST'])
def search_phrase_request():
    search_term = request.form["input"]
    filename = request.form["fileName"]
    res = es.search(
        index="books",
        size=20,
        body={
            "query": {
                "term" : { "fileName.keyword" : "Kimchy" },
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
        book = book.get('_source')
        toweb.append(book)
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


