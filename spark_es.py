
import time
import sys
import shutil
import glob
import json
# import random
import logging
# import boto3

# import pyspark.sql.functions as func
# from pyspark.sql.types import IntegerType, ArrayType
# from pyspark.ml import Pipeline
from pyspark import SparkContext

import os
from os.path import isfile, join
from os import environ as env
sys.path.extend(glob.glob(os.path.join(os.path.expanduser("~"), ".ivy2/jars/*.jar")))

from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession

def _start_spark():
    """ create and configure SparkSession; for SparkSQL
    :returns: SparkSession object
    """
    spark = SparkSession.builder.appName("readerApp") \
            .master("spark://ip-10-0-0-5.us-west-2.compute.internal:7077") \
            .config("spark.driver.memory","6G") \
            .config("spark.driver.maxResultSize", "2G") \
            .config("spark.executor.memory", "6G") \
            .config("spark.jar", "lib/sparknlp.jar") \
            .config("spark.kryoserializer.buffer.max", "500m") \
            .getOrCreate()
    return spark

def _set_env_vars():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars ../elasticsearch-hadoop-6.2.4/dist/elasticsearch-spark-20_2.11-6.2.4.jar pyspark-shell'

spark = _start_spark()

def _start_es():
    es=Elasticsearch(
            'ip-10-0-0-8:9200',
            http_auth=(env['ES_USER'], env['ES_PASS'])
            )
    if es.indices.exists('books'):
        es.indices.delete('books')
        es.indices.create('books')

def _set_es_conf(spark):
    es_write_conf = {
            # node sending data to (this should be the master)
            "es.nodes" : 'ip-10-0-0-8',
            "es.port" : '9200',
            # specify a resource in the form 'index/doc-type'
            "es.resource" : 'books/sentences',
            "es.input.json" : 'yes',
            # field in mapping used to specify the ES document ID
            "es.mapping.id": "doc_id",
            'es.net.http.auth.user': env['ES_USER'],
            'es.net.http.auth.pass': env['ES_PASS']
            }
    es_conf = spark.sparkContext.broadcast(es_write_conf)
    return es_write_conf


def format_data(data):
    return (data['doc_id'], json.dumps(data))

def main():

    _set_env_vars()
    _start_es()
    es_write_conf = _set_es_conf(spark)
    data = [
            {'some_key1': 'some_value1', 'doc_id': 123},
            {'some_key2': 'some_value2', 'doc_id': 456},
            {'some_key3': 'some_value3', 'doc_id': 789}
            ]
    rdd = spark.sparkContext.parallelize(data)

    rdd = rdd.map(lambda x: format_data(x))

    rdd.saveAsNewAPIHadoopFile(
            path='-',
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",

            # critically, we must specify our `es_write_conf`
            conf=es_write_conf)

    # Write to ES
    # _write_to_es(output, es_write_conf)
    spark.stop()

    # _read_es()

if __name__ == '__main__':
    main()
