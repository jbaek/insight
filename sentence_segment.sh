#!/bin/bash
spark-submit --jars ~/.ivy2/jars/JohnSnowLabs_spark-nlp-1.5.1.jar --master spark://ec2-35-155-202-75.us-west-2.compute.amazonaws.com:7077 sentence_segment.py 1000
