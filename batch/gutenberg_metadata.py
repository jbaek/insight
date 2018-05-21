import json
import logging
from gutenberg.query import get_etexts
from gutenberg.query import get_metadata
from gutenberg.query import list_supported_metadatas

print(list_supported_metadatas()) # prints (u'author', u'formaturi', u'language', ...)

def _extract_book_nums(keys):
    """ Extract book number from s3 key path for list of keys
    :param keys: list of keys from S3
    :returns: list of book numbers
    """
    book_nums = [key.replace('txt/', '').replace('.txt', '') for key in keys]
    return book_nums

def _extract_book_num(key):
    """ Extract book number for single key
    :param key: string
    :returns: book number as int
    """
    book_num = key.replace('txt/', '').replace('.txt', '')
    return book_num

def _get_uri_text_file(uris):
    """ filter out non-text files
    :param uris: list of uris for book
    :returns: list of string uri of text files
    """
    txt_uri = [uri for uri in uris if uri[-4:] == '.txt']
    return txt_uri

def _reformat_as_s3_key(txt_uri, filenames):
    filenames = [uri.split('/')[-1] for uri in txt_uri]
    filename = format('txt/{0}').format(filenames[0])
    return filename

def get_list_gutenberg_metadata(keys):
    output = []
    metadata = read_gutenberg_metadata_fromjson()
    for key in keys:
        book_dict = metadata.get(key)
        # book_dict = get_gutenberg_metadata(key)
        book_tuple = _convert_dict_to_tuple(book_dict)
        output.append(book_tuple)
    return output


def read_gutenberg_metadata_fromjson():
    output = []
    with open('metadata.json', 'r') as fp:
        books = json.load(fp)
    return books

def write_gutenberg_metadata_tojson(keys):
    output = {}
    for key in keys:
        book_dict = get_gutenberg_metadata(key)
        output[key] = book_dict
    with open('metadata.json', 'w') as fp:
        json.dump(output, fp)
    return output

def get_gutenberg_metadata(key):
    """ retrieve metadata for book numbers
    :param keys: list of keys from S3
    :returns: dictionary of books
    """
    book_num = _extract_book_num(key)
    queries = ['title', 'author', 'subject', 'formaturi']
    book_dict = {}
    book_dict['book_num'] = book_num

    for q in queries:
        val = get_metadata(q, book_num)
        if q == 'formaturi':
            txt_uri = _get_uri_text_file(val)
            if len(txt_uri) == 0:
                logging.info("No text file for {0}".format(book_num))
            else:
                book_dict['key'] = key
                book_dict['formaturi'] = txt_uri[0]
        elif len(val) > 1:
            book_dict[q] = list(val)
        elif len(val) == 1:
            book_dict[q] = list(val)[0]
    return book_dict

def _convert_dict_to_tuple(book_dict):
    fields = ['key', 'book_num', 'title', 'author', 'subject', 'formaturi']
    output = []
    for f in fields:
        output.append(book_dict.get(f))
    book_tuple = tuple(output)
    return book_tuple


def setup_logging(filename, loglevel):
    """
    :param filename: relative path and filename of log
    """
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(lineno)s - %(funcName)20s() %(message)s"
    logging.basicConfig(
            filename=filename,
            format=log_format,
            level=loglevel,
            filemode = 'w'
            )

if __name__ == '__main__':
    setup_logging('../log/gutenberg_metadata.log', logging.INFO)

    keys = ['txt/1.txt', 'txt/10.txt', 'txt/100.txt', 'txt/1342.txt']
    get_list_gutenberg_metadata(keys)
