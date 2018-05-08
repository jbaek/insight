import time
import sys
import os
from os.path import isfile, join
import shutil
import glob
import json
import random
import logging
import boto3
from boto.s3.connection import S3Connection

sys.path.extend(glob.glob(os.path.join(os.path.expanduser("~"), ".ivy2/jars/*.jar")))
from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import SentenceDetector, Tokenizer, Lemmatizer, SentimentDetector
from sparknlp.common import *
import pyspark.sql.functions as func
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.ml import Pipeline
from pyspark import SparkContext
from elasticsearch import Elasticsearch

TEXT_FOLDER = 'testing'
s3_bucket = "s3a://jason-b"

def _start_spark():
    """ create and configure SparkSession; for SparkSQL
    :returns: SparkSession object
    """
    spark = SparkSession.builder.appName("ner").master("local[1]").config("spark.driver.memory","8G").config("spark.driver.maxResultSize", "2G").config("spark.jar", "lib/sparknlp.jar").config("spark.kryoserializer.buffer.max", "500m").getOrCreate()
    return spark

spark = _start_spark()

s3resource = boto3.resource('s3')

def main():

    _set_env_vars()
    keys = _list_s3_files(s3resource, filetype=TEXT_FOLDER)
    with open('test_keys.txt', 'w') as keysfile:
        keysfile.write(json.dumps(keys, indent=4))
    # If the partitions are imbalanced, try to repartition
    parallel_keys = spark.sparkContext.parallelize(keys, numSlices=3)
    # activation = parallel_keys.map(lambda x: (x, x[0])) # _read_s3_file(x[0]))
    activation = parallel_keys.map(lambda x: x[0].replace('testing/', ''))
    _write_rdd_textfile(activation, 'txt/books')
    s3_obj = activation.map(lambda x: get_s3_object(x))
    _write_rdd_textfile(s3_obj, 'txt/objects')
    # logging.info([x for x in activation.first()])

    # testRDD = spark.sparkContext.wholeTextFiles("s3a://jason-b/testing/Charles Dickens___David Copperfield.txt", use_unicode=False)

    # books_df = _read_s3_files(spark, activation)
    # books_df.printSchema()
    # num_books = books_df.count()
    # logging.info("Number of books: {0}".format(num_books))

    # pipeline = _setup_pipeline()
    # output = _segment_sentences(books_df, pipeline)
    # _write_rdd_textfile(output.rdd, 'txt/books')

    # pipeline = _setup_sentiment_pipeline()
    # output = _sentiment_analysis(sentence_data, pipeline)
    # _write_rdd_textfile(output.rdd, 'txt/sentiment')

    # count_syllables = func.udf(lambda s: _udf_count_syllables_sentence(s), IntegerType())
    # exploded = output.select(func.monotonically_increasing_id().alias("doc_id"), func.size("sentence.result").alias("sentenceCount"), func.explode("sentence.result").alias("sentence"))
    # exploded = exploded.select("doc_id", "sentenceCount", count_syllables("sentence").alias('syllableCount'), "sentence")
    # exploded.show(20, False)

    # _write_rdd_textfile(exploded.rdd, 'txt/sentence')
    # exploded.groupby("doc_id").count().show()

    # sentence = output.select(["doc_id", "sentence"]).toJSON()
    # _write_rdd_textfile(sentence, 'txt/sentence')

    # Write entire array to ES
    # _start_es()
    # es_write_conf = _set_es_conf()
    # exploded = exploded.toJSON().map(lambda x: _format_data(x))
    # _write_to_es(exploded, es_write_conf)
    # spark.stop()

    # _read_es()
    # sentences = sentence.rdd.map(lambda s: s.sentence[0].result)
    # sentences = sentence.rdd.flatMap(lambda s: s.sentence)
    # results = sentence.rdd.map(lambda s: s.result).zipWithUniqueId()
    # _write_rdd_textfile(results, 'txt/results')

    # results.collect()
    # _write_to_es(output.rdd, es_write_conf)

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


def _set_env_vars():
    # set environment variable PYSPARK_SUBMIT_ARGS
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars ../elasticsearch-hadoop-6.2.4/dist/elasticsearch-spark-20_2.11-6.2.4.jar pyspark-shell'


def _list_s3_files(s3resource, filetype):
    """ List all files in S3; can't call wholeTextFiles by folder because it will overload the driver
    Need to call wholeTextFiles for each file and map to partition?
    :param s3resource: boto3 S3 resource object
    :param filetype: folder of S3 bucket
    :returns: list of tuples containing key and file size in S3
    """
    bucket = s3resource.Bucket('jason-b')
    fileslist = [(textfile.key, textfile.size) for textfile in bucket.objects.filter(Prefix=filetype)]
    # fileslist.remove('txt/')
    return fileslist


def map_func(key):
    # Use the key to read in the file contents, split on line endings
    for line in key.get_contents_as_string().splitlines():
        # parse one line of json
        j = json.loads(line)
        if "user_id" in j & "event" in j:
            if j['event'] == "event_we_care_about":
                yield j['user_id'], j['event']


def _read_s3_files(spark, book_rdd):
    s3_bucket = "s3a://jason-b"

    # books_df = create_testbook_df(spark)
    # books_df = books_df.union(book_rdd.toDF())

    # for bookfile in fileslist:
        # filepath = "{0}/{1}".format(s3_bucket, bookfile)
        # next_book_df = _read_s3_file(spark, filepath)
        # books_df = books_df.union(next_book_df)

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


def _udf_count_syllables_sentence(sentence):
    multisyllable_count = 0
    for word in sentence.split(' '):
        if _count_syllables_word(word) > 1:
            multisyllable_count += 1
    return multisyllable_count


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
    return numVowels
    # return len(word)


def _setup_pipeline():
    document_assembler = DocumentAssembler().setInputCol("rawDocument").setOutputCol("document").setIdCol("fileName")
    sentence_detector = SentenceDetector().setInputCols(["document"]).setOutputCol("sentence")
    # tokenizer = Tokenizer().setInputCols(["sentence"]).setOutputCol("token")
    pipeline = Pipeline().setStages([document_assembler, sentence_detector])
    return pipeline


def _segment_sentences(sentence_data, pipeline):
    output = pipeline.fit(sentence_data).transform(sentence_data)
    return output


def _setup_sentiment_pipeline():
    lexicon = 'lexicon.txt'
    document_assembler = DocumentAssembler().setInputCol("rawDocument").setOutputCol("document").setIdCol("doc_id")
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


def _format_data(x):
    """ Make elasticsearch-hadoop compatible"""
    data = json.loads(x)
    # data['doc_id'] = data.pop('count')
    test = (data['doc_id'], json.dumps(data))
    return test


def _write_rdd_textfile(rdd, folder):
    if os.path.isdir(folder):
        shutil.rmtree(folder)
    rdd.saveAsTextFile(folder)


def _start_es():
    es_cluster=["localhost:9200"]
    es=Elasticsearch(es_cluster,http_auth=('elastic','changeme'))
    if es.indices.exists('sentences'):
        es.indices.delete('sentences')
        es.indices.create('sentences')


def _write_to_es(rdd, es_write_conf):
    print(os.environ)
    rdd.saveAsNewAPIHadoopFile(
            path='-',
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=es_write_conf
            )


def _set_es_conf():
    es_write_conf = {
            # node sending data to (this should be the master)
            "es.nodes" : 'localhost',
            "es.port" : '9200',
            # specify a resource in the form 'index/doc-type'
            "es.resource" : 'sentences/testdoctype',
            "es.input.json" : "yes",
            # field in mapping used to specify the ES document ID
            "es.mapping.id": "doc_id"
            }
    return es_write_conf


def _read_es():
    sc = SparkContext(appName="PythonSparkReading")
    sc.setLogLevel("WARN")

    es_read_conf = {
            # node sending data to (should be the master)    
            "es.nodes" : "localhost:9200",
            # read resource in the format 'index/doc-type'
            "es.resource" : "sentences/testdoctype"
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
    FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
    logging.basicConfig(filename="log/sentence_segment.log", format=FORMAT, level=logging.INFO)
    start_time = time.time()
    main()
    end_time = time.time()
    logging.info("RUNTIME: {0}".format(end_time - start_time))
