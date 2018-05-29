#!/bin/bash
# spark-submit --jars ~/.ivy2/jars/JohnSnowLabs_spark-nlp-1.5.2.jar --master spark://ec2-54-190-217-16.us-west-2.compute.amazonaws.com:7077 sentence_segment.py 1000

spark-submit --py-files elastic.py --packages JohnSnowLabs:spark-nlp:1.5.1 --jars $PROJECT_DIR/jars/elasticsearch-spark-20_2.11-6.2.4.jar --master spark://ec2-54-189-221-234.us-west-2.compute.amazonaws.com:7077 sentence_segment.py --batchsize 50 &
