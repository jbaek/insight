""" AWS (S3) code related to batch pipeline
"""

import logging
import json

import boto3

S3BUCKET = 'jason-b'


def create_s3_resource():
    """ Create the s3 resource object with boto3
    :returns: s3 resource object
    """
    s3resource = boto3.resource('s3')
    return s3resource

def get_list_s3_files(s3resource, filetype, numrows=None):
    """ List all files in S3; can't call wholeTextFiles by folder because it will overload the driver
    Need to call wholeTextFiles for each file and map to partition?
    :param s3resource: boto3 S3 resource object
    :param filetype: folder of S3 bucket
    :returns: list of tuples containing key and file size in S3
    """
    try:
        bucket = s3resource.Bucket(S3BUCKET)
    except Exception as e:
        logging.info(e)
        raise e
    fileslist = [textfile.key for textfile in bucket.objects.filter(Prefix=filetype)]
    fileslist.remove('{0}/'.format(filetype))
    if numrows is None:
        numrows = len(fileslist)
    fileslist = fileslist[:numrows]
    logging.info("Num Files: {0}".format(len(fileslist)))
    logging.debug(json.dumps(fileslist, indent=4))
    return fileslist


def duplicate_books(batchsize=None):
    s3resource = create_s3_resource()
    keys = get_list_s3_files(
            s3resource,
            filetype='txt',
            numrows=batchsize
            )
    files = []
    for key in keys:
        filepath = key.split('/')
        for i in range(2, 21):
            newfile = "txt0{0}/{1}".format(i, filepath[1])
            s3resource.Object(S3BUCKET, newfile).copy_from(CopySource='{0}/{1}'.format(S3BUCKET, key))


def get_s3_object(key):
    obj = s3resource.Object(S3BUCKET, key)
    object_body = obj.get()['Body'].read().decode('utf-8')
    logging.info("OBJECT_BODY: " + object_body)
    return object_body


def read_s3_file(spark, filepath):
    booksRDD = spark.sparkContext.wholeTextFiles(filepath, use_unicode=False)
    # books_df = spark.createDataFrame(booksRDD, ["filepath", "rawDocument"])
    return booksRDD.map(lambda x: x[1])

if __name__ == '__main__':
    duplicate_books()
