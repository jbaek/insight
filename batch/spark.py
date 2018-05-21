""" PySpark code related to batch pipeline
"""
import logging
import json
from os import environ as env

from pyspark import SparkContext
from pyspark.sql import SparkSession

ES_NODES = [ip for ip in env['ES_NODES'].split(',')]
# NODES = ['localhost:9200'] # ['ip-10-0-0-5:9200'] #, 'ip-10-0-0-7', 'ip-10-0-0-11', 'ip-10-0-0-14']


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


def broadcast_es_write_config(spark_session, es_write_conf):
    """ Broadcast Elasticsearch configuration to Spark worker nodes
    :param spark: Spark Session
    """
    sc = spark_session.sparkContext
    es_conf = sc.broadcast(es_write_conf)
    es_user = sc.broadcast(env['ES_USER'])
    es_pass = sc.broadcast(env['ES_PASS'])
    es_nodes = sc.broadcast(ES_NODES)


def log_rdd(pbooks):
    """ Logging function for debugging purposes
    :param pbooks: RDD, one row per book
    """
    collected_books = pbooks \
            .collect()
            # .map(lambda x: x[1][:150]) \
    logging.info(json.dumps(collected_books[:1], indent=4))


def rdd_to_df(spark, books_rdd):
    books_df = spark.createDataFrame(books_rdd)
    testbook_df = create_testbook_df(spark)
    books_df = testbook_df.union(books_df)

    books_df = books_df.select(
            "filepath",
            func.substring_index("filePath", '/', -1).alias("fileName"),
            func.translate('rawDocument', '\n\r', '  ').alias("rawDocument")
            )
    return books_df


def create_testbook_df(spark):
    test_book = "Hi I heard about Spark. I wish Java\n\rcould use case classes. Logistic regression models are neat"
    books_df = spark.createDataFrame(
            [("999999", test_book)],
            ["filepath", "rawDocument"]
            )
    return books_df
