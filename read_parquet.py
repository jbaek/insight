import time
import sys
import os
sys.path.append('../../')
import glob
sys.path.extend(glob.glob(os.path.join(os.path.expanduser("~"), ".ivy2/jars/*.jar")))

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, PipelineModel
from sparknlp.annotator import *
from sparknlp.base import DocumentAssembler, Finisher

spark = SparkSession.builder.appName("ner").master("local[1]").config("spark.driver.memory","8G").config("spark.driver.maxResultSize", "2G").config("spark.jar", "lib/sparknlp.jar").config("spark.kryoserializer.buffer.max", "500m").getOrCreate()


#Load the input data to be annotated
data = spark.read.parquet("sentiment.parquet").limit(1000)
data.cache()
data.count()
data.show()

time.sleep(60)
