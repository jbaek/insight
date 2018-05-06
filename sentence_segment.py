import time
import sys
import os
import shutil
import glob
import json
from os.path import isfile, join
import random

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

TEXT_FOLDER = 'txt'

def main():
    _set_env_vars()
    spark = _start_spark()
    sentence_data = _read_files(spark)
    pipeline = _setup_pipeline()
    output = _segment_sentences(sentence_data, pipeline)

    # pipeline = _setup_sentiment_pipeline()
    # output = _sentiment_analysis(sentence_data, pipeline)
    # _write_rdd_textfile(output.rdd, 'txt/sentiment')

    output.printSchema()
    count_syllables = func.udf(lambda s: _udf_count_syllables_sentence(s), IntegerType())
    exploded = output.select(func.monotonically_increasing_id().alias("doc_id"), func.size("sentence.result").alias("sentenceCount"), func.explode("sentence.result").alias("sentence"))
    exploded = exploded.select("doc_id", "sentenceCount", count_syllables("sentence").alias('syllableCount'), "sentence")
    exploded.show(20, False)
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

def _format_data(x):
    """ Make elasticsearch-hadoop compatible"""
    # print(type(x))
    data = json.loads(x)
    # data['doc_id'] = data.pop('count')
    test = (data['doc_id'], json.dumps(data))
    return test

def _write_rdd_textfile(rdd, folder):
    if os.path.isdir(folder):
        shutil.rmtree(folder)
    rdd.saveAsTextFile(folder)


def _set_env_vars():
    # set environment variable PYSPARK_SUBMIT_ARGS
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars ../elasticsearch-hadoop-6.2.4/dist/elasticsearch-spark-20_2.11-6.2.4.jar pyspark-shell'

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


def _start_spark():
    spark = SparkSession.builder.appName("ner").master("local[1]").config("spark.driver.memory","8G").config("spark.driver.maxResultSize", "2G").config("spark.jar", "lib/sparknlp.jar").config("spark.kryoserializer.buffer.max", "500m").getOrCreate()
    return spark

def _read_files(spark):
    files = [f for f in os.listdir(TEXT_FOLDER) if isfile(join(TEXT_FOLDER, f))]
    print(files)
    sentence_data = spark.createDataFrame(
            [
                ("Hi I heard about Spark. I wish Java could use case classes. Logistic regression models are neat", 999999)
            ],
            ["rawDocument", "doc_id"]
            )
    for textfile in files:
        new_file = _read_file(spark, textfile)
        sentence_data = sentence_data.union(new_file)
    return sentence_data

def _read_file(spark, textfile):
    filepath = '{0}/{1}'.format(TEXT_FOLDER, textfile)
    print(filepath)
    with open(filepath, 'r') as content_file:
        content = content_file.read().replace('\n', ' ')
        content = content.replace('\r', ' ')
    return spark.createDataFrame([[content, random.randint(0, 1000)]])

def _setup_pipeline():
    document_assembler = DocumentAssembler().setInputCol("rawDocument").setOutputCol("document").setIdCol("doc_id")
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


if __name__ == '__main__':
    main()
