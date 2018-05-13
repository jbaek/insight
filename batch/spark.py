""" PySpark code related to batch pipeline
"""
import logging
import json
from os import environ as env

from pyspark import SparkContext
from pyspark.sql import SparkSession

ES_NODES = [ip for ip in env['ES_NODES'].split(',')]
# NODES = ['localhost:9200'] # ['ip-10-0-0-5:9200'] #, 'ip-10-0-0-7', 'ip-10-0-0-11', 'ip-10-0-0-14']
ES_MASTER_NODE = 'ip-10-0-0-8'


def create_spark_session():
    """ create and configure SparkSession; for SparkSQL
    :returns: SparkSession object
    """
    try:
        spark = SparkSession.builder.appName("readerApp") \
                .master("spark://ip-10-0-0-13.us-west-2.compute.internal:7077") \
                .config("spark.driver.memory","6G") \
                .config("spark.driver.maxResultSize", "2G") \
                .config("spark.executor.memory", "6G") \
                .config("spark.jar", "lib/sparknlp.jar") \
                .config("spark.kryoserializer.buffer.max", "500m") \
                .getOrCreate()
        return spark
    except Exception as e:
        logging.error(e)
        raise e


def broadcast_es_write_config(spark):
    """ Broadcast Elasticsearch configuration to Spark worker nodes
    :param spark: Spark Session
    :returns: dict of ES config settings for writes
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
            # "es.nodes.client.only": 'true',
            # "es.nodes.wan.only": 'yes',
            # "es.nodes.discovery": 'false',
            }
    es_conf = spark.sparkContext.broadcast(es_write_conf)
    return es_write_conf


def log_rdd(pbooks):
    """ Logging function for debugging purposes
    :param pbooks: RDD, one row per book
    """
    collected_books = pbooks \
            .map(lambda x: x[1][:150]) \
            .collect()
    logging.info(json.dumps(collected_books[:5], indent=4))
