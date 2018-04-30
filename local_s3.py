import boto3

BOOKPATH = '/newvolume/ebooks'

def list_buckets(resource):
    for bucket in resource.buckets.all():
        print(bucket.name)

def main():
    bucket = 'jason-b'
    s3 = boto3.resource('s3')
    list_buckets(s3)
    bookfile = '10897.txt'
    data = open('{0}/{1}'.format(BOOKPATH, bookfile), 'rb')
    s3.Bucket(bucket).put_object(Key='testing/' + bookfile, Body=data)
    # s3.Object(bucket, bookfile).put(Body=open('/tmp/hello.txt', 'rb'))

if __name__ == '__main__':
    main()
