""" Elasticsearch code related to batch pipeline
"""

from elasticsearch import Elasticsearch, ConnectionError
import logging
from os import environ as env

ES_MASTER_NODE = 'ip-10-0-0-8'
# ['ip-10-0-0-8:9200','ip-10-0-0-10:9200','ip-10-0-0-6:9200','ip-10-0-0-12:9200']

def check_elasticsearch():
    """ Check that ElasticSearch nodes are up
    :returns: python Elasticsearch object
    """
    ES_USER = env['ES_USER']
    ES_PASS = env['ES_PASS']
    ES_NODES = [ip for ip in env['ES_NODES'].split(',')]
    try:
        logging.info(ES_USER)
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


def set_es_write_conf():
    """ set configuration for writing books to ElasticSearch
    :returns: dict of configuration
    """
    es_write_conf = {
            # node sending data to (this should be the master)
            "es.nodes" : ES_MASTER_NODE,
            "es.port" : '9200',
            # specify a resource in the form 'index/doc-type'
            "es.resource" : 'books/sentences',
            "es.input.json" : 'yes',
            # field in mapping used to specify the ES document ID
            "es.mapping.id": "sentence_id",
            'es.net.http.auth.user': env['ES_USER'],
            'es.net.http.auth.pass': env['ES_PASS']
            # 'es.batch.size.bytes': '200mb'
            # 'es.batch.size.entries': '500'
            # "es.nodes.client.only": 'true',
            # "es.nodes.wan.only": 'yes',
            # "es.nodes.discovery": 'false',
            }
    return es_write_conf


def read_es():
    sc = SparkContext(appName="PythonSparkReading")
    sc.setLogLevel("WARN")

    es_read_conf = {
            # node sending data to (should be the master)    
            "es.nodes" : "localhost:9200",
            # read resource in the format 'index/doc-type'
            "es.resource" : "books/sentences"
            }

    es_rdd = sc.newAPIHadoopRDD(
            inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=es_read_conf
            )

    first_five = es_rdd.take(2)
    print(first_five)
    es_rdd = es_rdd.map(lambda x: x[1])
    es_rdd.take(1)
    sc.stop()
