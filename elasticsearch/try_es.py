import os
import json
from pyspark import SparkContext
from elasticsearch import Elasticsearch

# set environment variable PYSPARK_SUBMIT_ARGS
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars ../../elasticsearch-hadoop-6.2.4/dist/elasticsearch-spark-20_2.11-6.2.4.jar pyspark-shell'

es_cluster=["localhost:9200"]
# es_cluster=["ec2-35-155-202-75.us-west-2.compute.amazonaws.com"]
es=Elasticsearch(es_cluster,http_auth=('elastic','changeme'))
if es.indices.exists('testindex'):
    es.indices.delete('testindex')
    es.indices.create('testindex')

# invoke SparkContext (using environment variable)
sc = SparkContext(appName="PythonSparkStreaming")

es_write_conf = {
        # node sending data to (this should be the master)
        "es.nodes" : 'localhost',
        # specify the port in case not the default port
        "es.port" : '9200',
        # specify a resource in the form 'index/doc-type'
        "es.resource" : 'testindex/testdoc',
        # is the input JSON?
        "es.input.json" : "yes",
        # field in mapping used to specify the ES document ID
        "es.mapping.id": "doc_id"
        }

data = [
        {'some_key': 'some_value_123', 'doc_id': 123},
        {'some_key': 'some_value_456', 'doc_id': 456},
        {'some_key': 'some_value_789', 'doc_id': 789}
        ]

rdd = sc.parallelize(data)

def format_data(x):
    """ Make elasticsearch-hadoop compatible"""
    data = x
    # data['doc_id'] = data.pop('count')
    return (data['doc_id'], json.dumps(data))

rdd = rdd.map(lambda x: format_data(x))

# Write to ES
rdd.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",

        # critically, we must specify our `es_write_conf`
        conf=es_write_conf
        )
sc.stop()

sc = SparkContext(appName="PythonSparkReading")
sc.setLogLevel("WARN")

es_read_conf = {
        # node sending data to (should be the master)    
        "es.nodes" : "localhost:9200",
        # read resource in the format 'index/doc-type'
        "es.resource" : "testindex/testdoc"
        }

es_rdd = sc.newAPIHadoopRDD(
        inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_read_conf
        )

first_five = es_rdd.take(5)
print(first_five)
es_rdd = es_rdd.map(lambda x: x[1])
es_rdd.take(1)

