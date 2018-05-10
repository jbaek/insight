#!/bin/bash

spark-submit --jars jars/elasticsearch-spark-20_2.11-6.2.4.jar --master spark://ec2-54-189-221-234.us-west-2.compute.amazonaws.com:7077 spark_es.py 
