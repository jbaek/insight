import time
import sys
import os
import shutil
import glob
from os.path import isfile, join

sys.path.extend(glob.glob(os.path.join(os.path.expanduser("~"), ".ivy2/jars/*.jar")))
from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import SentenceDetector
from sparknlp.common import *
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from elasticsearch import Elasticsearch

TEXT_FOLDER = 'txt'

def main():
    _set_env_vars()
    spark = _start_spark()
    sentence_data = _read_files(spark)
    pipeline = _setup_pipeline()
    output = _segment_sentences(sentence_data, pipeline)
    output.printSchema()
    sentence = output.select("sentence")
    # sentences = sentence.rdd.map(lambda s: s.sentence[0].result)
    sentences = sentence.rdd.flatMap(lambda s: s.sentence)
    results = sentences.map(lambda s: s.result)
    shutil.rmtree('txt/sentence')
    results.saveAsTextFile("txt/sentence")
    results.collect()
    # _start_es()
    # es_write_conf = _set_es_conf()
    # _write_to_es(output.rdd, es_write_conf)
    # output.select("sentence").show()
    # shutil.rmtree('txt/sentence')
    # output.select("sentence").rdd.saveAsTextFile("txt/sentence")

def _set_env_vars():
    # set environment variable PYSPARK_SUBMIT_ARGS
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars ../../elasticsearch-hadoop-6.2.4/dist/elasticsearch-spark-20_2.11-6.2.4.jar pyspark-shell'

def _start_es():
    es_cluster=["localhost:9200"]
    es=Elasticsearch(es_cluster,http_auth=('elastic','changeme'))
    if es.indices.exists('sentences'):
        es.indices.delete('sentences')
        es.indices.create('sentences')

def _write_to_es(rdd, es_write_conf):
    rdd.saveAsNewAPIHadoopFile(
            path='-',
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",

            # critically, we must specify our `es_write_conf`
            conf=es_write_conf
            )

def _set_es_conf():
    es_write_conf = {
            # node sending data to (this should be the master)
            "es.nodes" : 'localhost',
            # specify the port in case not the default port
            "es.port" : '9200',
            # specify a resource in the form 'index/doc-type'
            "es.resource" : 'testindex/testdoc',
            # is the input JSON?
            # "es.input.json" : "yes",
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
                ("Hi I heard about Spark. I wish Java could use case classes. Logistic regression models are neat", )
            ],
            ["rawDocument"]
            )
    for textfile in files:
        new_file = _read_file(spark, textfile)
        sentence_data = sentence_data.union(new_file)
    return sentence_data

def _read_file(spark, textfile):
    filepath = '{0}/{1}'.format(TEXT_FOLDER, textfile)
    print(filepath)
    with open(filepath, 'r') as content_file:
        content = content_file.read().replace('\n', '')
        content = content.replace('\r', '')
    return spark.createDataFrame([[content]])

def _setup_pipeline():
    document_assembler = DocumentAssembler().setInputCol("rawDocument").setOutputCol("document")
    sentence_detector = SentenceDetector().setInputCols(["document"]).setOutputCol("sentence")
    pipeline = Pipeline().setStages([document_assembler, sentence_detector])
    return pipeline

def _segment_sentences(sentence_data, pipeline):
    output = pipeline.fit(sentence_data).transform(sentence_data)
    return output


if __name__ == '__main__':
    main()
