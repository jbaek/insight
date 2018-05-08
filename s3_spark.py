import json
import logging
import boto3
s3resource = boto3.resource('s3')

def map_func(key):
    yield key, booktext

def test_map_func(key):
    bucket = 'jason-b'
    key = 'testing/10897.txt'
    obj = s3resource.Object(bucket, key)
    booktext = obj.get()['Body'].read().decode('utf-8')
    return booktext[:100]

def main():

    bucket = s3resource.Bucket('jason-b')
    logging.info(test_map_func('1'))

if __name__ == '__main__':
    FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
    logging.basicConfig(filename="log/s3_spark.log", format=FORMAT, filemode='w', level=logging.INFO)

    main()
