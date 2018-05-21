import time
import os
from os import environ as env
from os.path import isfile, join
import shutil
import json
import random
import logging
import argparse
import sys

import boto3

import utils
import spark
import aws
import spark_nlp
import elastic
import gutenberg_metadata as gm

import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType, ArrayType

PROJECT_DIR = env['PROJECT_DIR']
TEXT_FOLDER = 'txt/'
NUM_PARTITIONS = 6


def main():
    logfile = "{0}/log/sentence_segment.log".format(PROJECT_DIR)
    utils.setup_logging(logfile, logging.INFO)
    # logging.info(sys.path)

    args = utils.parse_arguments()
    batchsize = args.batchsize

    start_time = time.time()

    s3resource = aws.create_s3_resource()
    keys = aws.get_list_s3_files(
            s3resource,
            filetype=TEXT_FOLDER,
            numrows=batchsize
            )

    # metadata = gm.write_gutenberg_metadata_tojson(keys)
    # keys_metadata = gm.read_gutenberg_metadata_fromjson()
    # print(json.dumps(keys_metadata, indent=4))
    keys_metadata = gm.get_list_gutenberg_metadata(keys)

    spark_session = spark.create_spark_session()

    es = elastic.check_elasticsearch()
    es_write_conf = elastic.set_es_write_conf()
    spark.broadcast_es_write_config(spark_session, es_write_conf)

    pbooks = s3_to_rdd(spark_session, keys_metadata)
    # spark.log_rdd(pbooks)
    # write_rdd_textfile(pbooks, '../txt/test')

    pipeline = spark_nlp.setup_pipeline()
    books = spark_nlp.segment_sentences(spark_session, pbooks, pipeline)
    books.printSchema()

    # Go from one book per row to one sentence per row
    sentences = books.select(
            func.monotonically_increasing_id().alias("sentence_id"),
            func.col("fileName"),
            func.posexplode("sentence.result").alias("position", "sentenceText"),
            func.size("sentence.result").alias("numSentencesInBook"),
            "title",
            "author",
            "subjects",
            "uri"
            )
    # logging.info("Num Sentences: {0}".format(sentences.count()))

    sentences = spark_nlp.tokenize_sentences(sentences)

    count_syllables_udf = func.udf(
            lambda s: _udf_sentence_count_syllables_sentence(s),
            ArrayType(IntegerType())
            )
    count_sentence_multisyllables_udf = func.udf(
            lambda s: sum(_udf_sentence_count_syllables_sentence(s)),
            IntegerType()
            )
    count_array_multisyllables_udf = func.udf(
            lambda a: sum(_udf_array_count_syllables_sentence(a)),
            IntegerType()
            )
    sentences = sentences.select(
            "sentence_id",
            "fileName",
            "position",
            "sentenceText",
            "numSentencesInBook",
            "title",
            "author",
            "subjects",
            "uri",
            # count_sentence_multisyllables_udf('sentenceText') \
                    # .alias("multiSyllableCount")
            count_array_multisyllables_udf("words") \
                    .alias("multiSyllableCount"),
            func.size("words").alias("numWordsInSentence"),
            )
    sentences.printSchema()

    # pipeline = spark_nlp.setup_sentiment_pipeline()
    # output = spark_nlp.sentiment_analysis(sentence_data, pipeline)


    # Format to load ElasticSearch
    sentences = sentences.select(
            "sentence_id",
            func.to_json(func.struct(
                "sentence_id",
                "fileName",
                "position",
                "sentenceText",
                "numSentencesInBook",
                "multiSyllableCount",
                "numWordsInSentence",
                "title",
                "author",
                "subjects",
                "uri"
                )
                ).alias("value")
            )
    # sentence = output.select(["sentence_id", "sentence"]).toJSON()
    sentences = sentences.rdd.map(lambda x: elastic.format_data(x))

    # Write to ES
    write_rdd_to_es(sentences, es_write_conf)

    spark_session.stop()
    end_time = time.time()
    logging.info("RUNTIME: {0}".format(end_time - start_time))


def set_env_vars():
    # set environment variable PYSPARK_SUBMIT_ARGS
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars ../elasticsearch-hadoop-6.2.4/dist/elasticsearch-spark-20_2.11-6.2.4.jar pyspark-shell'


def s3_to_rdd(spark_session, keys):
    """ For every key pull contents of s3 files to Spark partition
    :param spark_session: SparkSession object
    :param keys: list of files in s3
    :returns: spark RDD containing one row per book (filename, content)
    """
    pkeys = spark_session.sparkContext.parallelize(keys) # , numSlices=NUM_PARTITIONS)
    logging.debug(json.dumps(pkeys.collect(), indent=4))
    pbooks = pkeys.flatMap(_get_s3file_map_func)
    logging.info("Number of partitions: {0}".format(pbooks.getNumPartitions()))
    return pbooks


def _get_s3file_map_func(key):
    """ read contents of file and remove newlines
    :params key: name of file in s3
    :returns: iterator object of tuple (key, value)
    """
    s3_client = boto3.client('s3')
    try:
        text = boto3.client('s3') \
                .get_object(Bucket="jason-b", Key=key[0])['Body'] \
                .read() \
                .decode('utf-8') \
                .replace("\n", " ")
        # output = tuple(list(key).append(text))
        yield key + (text,)
    except:
        logging.info("Key: {0}".format(key))

def _udf_array_count_syllables_sentence(token_array):
    return [_count_syllables_word(word) for word in token_array]


def _udf_string_count_syllables_sentence(sentence_string):
    return [_count_syllables_word(word) for word in sentence_string.split(" ")]
    # multisyllable_count = 0
    # for word in token_array:
        # if _count_syllables_word(word) > 1:
            # multisyllable_count += 1
    # return multisyllable_count


def _count_syllables_word(word):
    vowels = "aeiouy"
    numVowels = 0
    lastWasVowel = False
    for wc in word:
        foundVowel = False
        for v in vowels:
            if v == wc:
                if not lastWasVowel:
                    numVowels+=1   #don't count diphthongs
                foundVowel = lastWasVowel = True
                break
        if not foundVowel:  #If full cycle and no vowel found, set lastWasVowel to false
            lastWasVowel = False
    if len(word) > 2 and word[-2:] == "es": #Remove es - it's "usually" silent (?)
        numVowels-=1
    elif len(word) > 1 and word[-1:] == "e":    #remove silent e
        numVowels-=1
    multiSyllables = 0
    if numVowels > 1:
        multiSyllables = 1
    return multiSyllables


def write_rdd_textfile(rdd, folder):
    if os.path.isdir(folder):
        shutil.rmtree(folder)
    rdd.saveAsTextFile(folder)


def write_rdd_to_es(rdd, es_write_conf):
    sys.path.append("{0}/batch".format(PROJECT_DIR))
    rdd.saveAsNewAPIHadoopFile(
            path='-',
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=es_write_conf
            )


if __name__ == '__main__':
    main()
