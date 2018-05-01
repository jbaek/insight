import boto3
import glob
import os

# BOOKPATH = '/newvolume/ebooks'
# S3FOLDER = 'txt'
BUCKET = 'jason-b'

S3CONFIG = {
        'ebooks': {
            'sourcefolder': '/newvolume/ebooks',
            's3folder': 'txt'
            },
        'wikipedia': {
            'sourcefolder': '/newvolume/wikipedia',
            's3folder': 'wikipedia'
            }
        }


def list_buckets(resource):
    """ print all buckets in s3
    """
    for bucket in resource.buckets.all():
        print(bucket.name)


def get_list_files(filetype):
    """ list all txt files in directory of filetype
    :param filetype: see keys of S3CONFIG
    :returns: list of paths of txt files
    """
    sourcefolder = S3CONFIG[filetype]['sourcefolder']
    files = os.listdir(sourcefolder)
    s3files = []
    for root, dirs, files in os.walk(sourcefolder):
        subfolder = root.replace(sourcefolder, '')
        if subfolder:
            subfolder = subfolder.replace('/', '')
            files_infolder = ["{0}_{1}".format(subfolder, filename) for filename in files]
        else:
            files_infolder = [filename for filename in files if not filename.endswith(".bz2")]
        s3files = s3files + files_infolder
    print(s3files[:10])
    print(len(s3files))


def upload_s3(s3, bookfile):
    print(bookfile)
    data = open('{bookpath}/{book}'.format(
        bookpath=BOOKPATH,
        book=bookfile),
        'rb')
    s3.Bucket(BUCKET).put_object(Key='{0}/{1}'.format(S3FOLDER, bookfile), Body=data)

def main():
    s3 = boto3.resource('s3')
    list_buckets(s3)
    wikifiles = get_list_files('wikipedia')
    bookfiles = get_list_files('ebooks')
    # bookfiles = ['15445.txt', '10442.txt']
    # for bookfile in bookfiles:
        # upload_s3(s3, bookfile)


if __name__ == '__main__':
    main()
