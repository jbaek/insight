import time
import sys
import os
import shutil
import glob
sys.path.extend(glob.glob(os.path.join(os.path.expanduser("~"), ".ivy2/jars/*.jar")))
from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import SentenceDetector
from sparknlp.common import *
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline

def main():
    spark = _start_spark()
    sentence_data = _read_file(spark)
    pipeline = _setup_pipeline()
    output = _segment_sentences(sentence_data, pipeline)
    output.show()
    output.printSchema()
    output.select("sentence").show()
    shutil.rmtree('txt/sentence')
    output.select("sentence").rdd.saveAsTextFile("txt/sentence")

def _start_spark():
    spark = SparkSession.builder.appName("ner").master("local[1]").config("spark.driver.memory","8G").config("spark.driver.maxResultSize", "2G").config("spark.jar", "lib/sparknlp.jar").config("spark.kryoserializer.buffer.max", "500m").getOrCreate()
    return spark

def _read_file(spark):
    with open('txt/James Joyce___Ulysses.txt', 'r') as content_file:
        content = content_file.read().strip()
        sentence_data = spark.createDataFrame(
                [
                    (1, "Hi I heard about Spark. I wish Java could use case classes. Logistic regression models are neat"),
                    (2, content)
                ],
                ["id", "rawDocument"]
                )
    return sentence_data

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
