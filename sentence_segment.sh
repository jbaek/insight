#!/bin/bash
# spark-submit --jars ~/.ivy2/jars/JohnSnowLabs_spark-nlp-1.5.2.jar --master spark://ec2-54-190-217-16.us-west-2.compute.amazonaws.com:7077 sentence_segment.py 1000

spark-submit --packages JohnSnowLabs:spark-nlp:1.5.1 --master spark://ec2-54-190-217-16.us-west-2.compute.amazonaws.com:7077 sentence_segment.py 1000


