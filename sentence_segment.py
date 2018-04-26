import time
import sys
import os

import glob
sys.path.extend(glob.glob(os.path.join(os.path.expanduser("~"), ".ivy2/jars/*.jar")))
from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import SentenceDetector
from sparknlp.common import *
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline

#Load the input data to be annotated
# data = spark.read.parquet("file:///" + os.getcwd() + "/../../../src/test/resources/sentiment.parquet").limit(1000)
# data.cache()
# data.count()
# data.show()

spark = SparkSession.builder.appName("ner").master("local[1]").config("spark.driver.memory","8G").config("spark.driver.maxResultSize", "2G").config("spark.jar", "lib/sparknlp.jar").config("spark.kryoserializer.buffer.max", "500m").getOrCreate()

sentence_data = spark.createDataFrame([(1, "Hi I heard about Spark"), (2, "I wish Java could use case classes"), (3, "Logistic regression models are neat")], ["id", "rawDocument"])

document_assembler = DocumentAssembler().setInputCol("rawDocument").setOutputCol("document")
sentence_detector = SentenceDetector().setInputCols(["document"]).setOutputCol("sentence")

pipeline = Pipeline().setStages([document_assembler, sentence_detector])

output = pipeline.fit(sentence_data).transform(sentence_data)
output.show()
