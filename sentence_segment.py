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

TEXT_FOLDER = 'txt'

def main():
    spark = _start_spark()
    sentence_data = _read_files(spark)
    pipeline = _setup_pipeline()
    output = _segment_sentences(sentence_data, pipeline)
    output.printSchema()
    output.select("sentence").show()
    shutil.rmtree('txt/sentence')
    output.select("sentence").rdd.saveAsTextFile("txt/sentence")

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
