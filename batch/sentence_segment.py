import time
import os
from os import environ as env
from os.path import isfile, join
import shutil
import json
import random
import logging
import argparse

import boto3

import utils
import elastic
import spark
import aws
import spark_nlp

import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType, ArrayType

PROJECT_DIR = env['PROJECT_DIR']
TEXT_FOLDER = 'txt'
s3_bucket = "s3a://jason-b"
NUM_PARTITIONS = 6


def main():
    logfile = "{0}/log/sentence_segment.log".format(PROJECT_DIR)
    utils.setup_logging(logfile, logging.INFO)

    args = utils.parse_arguments()
    batchsize = args.batchsize

    start_time = time.time()

    # _set_env_vars()

    spark_session = spark.create_spark_session()

    es = elastic.check_elasticsearch()
    es_write_conf = spark.broadcast_es_write_config(spark_session)

    s3resource = aws.create_s3_resource()
    keys = aws.get_list_s3_files(
            s3resource,
            filetype=TEXT_FOLDER,
            numrows=batchsize
            )

    pbooks = s3_to_rdd(spark_session, keys)
    # testing_rdd = spark.sparkContext.wholeTextFiles("s3a://jason-b/{0}".format(TEXT_FOLDER), minPartitions=6, use_unicode=False)
    # spark.log_rdd(pbooks)

    pipeline = spark_nlp.setup_pipeline()
    books = spark_nlp.segment_sentences(spark_session, pbooks, pipeline)

    # Go from one book per row to one sentence per row
    sentences = books.select(
            func.monotonically_increasing_id().alias("sentence_id"),
            func.col("fileName"),
            func.posexplode("sentence.result").alias("position", "sentenceText"),
            func.size("sentence.result").alias("numSentencesInBook"),
            # func.col("token.result").alias('book_tokens')
            # token_lengths_udf('token.result').alias("tokenLengths")
            )
    sentences = spark_nlp.tokenize_sentences(sentences)
    logging.info("Num Sentences: {0}".format(sentences.count()))
    logging.info(sentences.first())

    """
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
            # count_sentence_multisyllables_udf('sentenceText') \
                    # .alias("multiSyllableCount")
            count_array_multisyllables_udf("book_tokens") \
                    .alias("multiSyllableCount")
            )
    sentences = sentences.select(
            "sentence_id",
            func.to_json(func.struct(
                "sentence_id",
                "fileName",
                "sentenceText",
                "position",
                "numSentencesInBook",
                "multiSyllableCount"
                )
                ).alias("value")
            )
    # logging.info(sentences.collect())
    # tokens = output.select(
            # func.col("fileName").alias("sentence_id"),
            # # count_syllables_udf('token.result').alias("syllableCounts"),
            # count_multisyllables_udf('token.result').alias("multiSyllableCount")
            # )

    sentences.printSchema()
    # logging.info(sentences.rdd.collect())
    # _write_rdd_textfile(sentences.rdd, 'txt/sentences')

    # pipeline = _setup_sentiment_pipeline()
    # output = _sentiment_analysis(sentence_data, pipeline)
    # _write_rdd_textfile(output.rdd, 'txt/sentiment')

    # sentence = output.select(["sentence_id", "sentence"]).toJSON()
    # _write_rdd_textfile(sentence, 'txt/sentence')

    # Write to ES
    logging.info("FORMAT FOR HADOOP")

    sentences = sentences.rdd.map(lambda x: _format_data(x))
    # logging.info(sentences.collect())
    # _write_to_es(sentences, es_write_conf)

    # _read_es()
    # sentences = sentence.rdd.map(lambda s: s.sentence[0].result)
    # sentences = sentence.rdd.flatMap(lambda s: s.sentence)
    # results = sentence.rdd.map(lambda s: s.result).zipWithUniqueId()

    """
    spark_session.stop()
    end_time = time.time()
    logging.info("RUNTIME: {0}".format(end_time - start_time))


def _set_env_vars():
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
    text = boto3.client('s3') \
            .get_object(Bucket="jason-b", Key=key)['Body'] \
            .read() \
            .decode('utf-8') \
            .replace("\n", " ")
    yield (key, text)


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


def get_s3_object(key):
    bucket = 'jason-b'
    obj = s3resource.Object(bucket, key)
    object_body = obj.get()['Body'].read().decode('utf-8')
    logging.info("OBJECT_BODY: " + object_body)
    return object_body

def _read_s3_file(filepath):
    booksRDD = spark.sparkContext.wholeTextFiles(filepath, use_unicode=False)
    # books_df = spark.createDataFrame(booksRDD, ["filepath", "rawDocument"])
    return booksRDD.map(lambda x: x[1])


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


def _setup_sentiment_pipeline():
    lexicon = 'lexicon.txt'
    document_assembler = DocumentAssembler().setInputCol("rawDocument").setOutputCol("document").setIdCol("sentence_id")
    sentence_detector = SentenceDetector().setInputCols(["document"]).setOutputCol("sentence")
    tokenizer = Tokenizer().setInputCols(["sentence"]).setOutputCol("token")
    lemmatizer = Lemmatizer().setInputCols(["token"]).setOutputCol("lemma").setDictionary("txt/corpus/lemmas_small.txt", key_delimiter="->", value_delimiter="\t")
    sentiment_detector = SentimentDetector().setInputCols(["lemma", "sentence"]).setOutputCol("sentiment_score").setDictionary("txt/corpus/{0}".format(lexicon), ",")
    finisher = Finisher().setInputCols(["sentiment_score"]).setOutputCols(["sentiment"])
    pipeline = Pipeline(stages=[document_assembler, sentence_detector, tokenizer, lemmatizer, sentiment_detector, finisher])
    return pipeline


def _sentiment_analysis(data, pipeline):
    model = pipeline.fit(data)
    result = model.transform(data)
    return result


def _write_rdd_textfile(rdd, folder):
    if os.path.isdir(folder):
        shutil.rmtree(folder)
    rdd.saveAsTextFile(folder)


def _format_data(x):
    """ Make elasticsearch-hadoop compatible"""
    # data = json.loads(x)
    # test = (data['sentence_id'], json.dumps(data))
    test = (x[0], x[1])
    return test

def _write_to_es(rdd, es_write_conf):
    # print(os.environ)
    rdd.saveAsNewAPIHadoopFile(
            path='-',
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=es_write_conf
            )


def _read_es():
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



if __name__ == '__main__':
    print('hello')
    main()
