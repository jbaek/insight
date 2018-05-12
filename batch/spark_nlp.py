""" Spark-NLP code related to batch pipeline
"""
import sys
from glob import glob
from os.path import join, expanduser

sys.path.extend(glob(join(expanduser("~"), ".ivy2/jars/*.jar")))
from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import SentenceDetector, Tokenizer, Lemmatizer, SentimentDetector
from sparknlp.common import *
from pyspark.ml import Pipeline


def setup_pipeline():
    document_assembler = DocumentAssembler(). \
            setInputCol("rawDocument"). \
            setOutputCol("document"). \
            setIdCol("fileName")
    sentence_detector = SentenceDetector(). \
            setInputCols(["document"]). \
            setOutputCol("sentence")
    # tokenizer = Tokenizer(). \
            # setInputCols(["sentence"]). \
            # setOutputCol("token")
    pipeline = Pipeline(). \
            setStages([
                document_assembler,
                sentence_detector
#                tokenizer
                ])
    return pipeline


def segment_sentences(spark_session, books_rdd, pipeline):
    books_df = spark_session.createDataFrame(books_rdd, ["fileName", "rawDocument"])
    output = pipeline. \
            fit(books_df). \
            transform(books_df)
    return output
