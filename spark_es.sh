#!/bin/bash

spark-submit --jars ../elasticsearch-hadoop-6.2.4/dist/elasticsearch-spark-20_2.11-6.2.4.jar --master spark://ec2-54-190-217-16.us-west-2.compute.amazonaws.com:7077 spark_es.py 
