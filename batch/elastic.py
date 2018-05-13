""" Elasticsearch code related to batch pipeline
"""

from elasticsearch import Elasticsearch, ConnectionError
import logging
from os import environ as env

ES_USER = env['ES_USER']
ES_PASS = env['ES_PASS']
ES_NODES = [ip for ip in env['ES_NODES'].split(',')]
# ['ip-10-0-0-8:9200','ip-10-0-0-10:9200','ip-10-0-0-6:9200','ip-10-0-0-12:9200']

def check_elasticsearch():
    """ Check that ElasticSearch nodes are up
    :returns: python Elasticsearch object
    """
    try:
        es = Elasticsearch(ES_NODES, http_auth=(ES_USER, ES_PASS))
        es.info()
    except ConnectionError as ce:
        logging.error(ce)

    # if es.indices.exists('books'):
        # es.indices.delete('books')
        # es.indices.create('books')

    return es

def format_data(x):
    """ Make elasticsearch-hadoop compatible"""
    # data = json.loads(x)
    # test = (data['sentence_id'], json.dumps(data))
    test = (x[0], x[1])
    return test

