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
from pyspark.ml.feature import RegexTokenizer


def setup_pipeline():
    """ create a Spark pipeline to ingest raw text then  split into sentences
    :returns: Spark ML pipeline
    """

    document_assembler = DocumentAssembler(). \
            setInputCol("rawDocument"). \
            setOutputCol("document"). \
            setIdCol("fileName")
    sentence_detector = SentenceDetector(). \
            setInputCols(["document"]). \
            setOutputCol("sentence")
    pipeline = Pipeline(). \
            setStages([
                document_assembler,
                sentence_detector
                ])
    return pipeline


def segment_sentences(spark_session, books_rdd, pipeline):
    """ Take a RDD with one book per row, run the Spark pipeline and return
    a dataframe with one sentence per row
    :param spark_session: SparkSession object
    :param books_rdd: input data RDD
    :param pipeline: SparkML pipeline
    :returns: data frame
    """
    books_df = spark_session.createDataFrame(books_rdd, ["fileName", "rawDocument"])
    output = pipeline. \
            fit(books_df). \
            transform(books_df)
    return output

def tokenize_sentences(sentences_df):
    """ Used Spark ML tokenizer to tokenize each sentence
    :param sentences_df: one sentence per row
    :returns: same data frame with added column with tokenized array
    """
    regexTokenizer = RegexTokenizer(
            inputCol="sentenceText",
            outputCol="words",
            pattern="\\W"
            )
    tokenized = regexTokenizer.transform(sentences_df)
    return tokenized
