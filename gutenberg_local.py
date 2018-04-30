"""
from gutenberg.acquire import get_metadata_cache
cache = get_metadata_cache()
cache.populate()

from gutenberg.query import list_supported_metadatas
print(list_supported_metadatas())

from gutenberg.query import get_etexts
from gutenberg.query import get_metadata
print(get_metadata('title', 2701))  # prints frozenset([u'Moby Dick; Or, The Whale'])
print(get_metadata('author', 2701)) # prints frozenset([u'Melville, Hermann'])
print(get_etexts('title', 'Moby Dick; Or, The Whale'))  # prints frozenset([2701, ...])
print(get_etexts('author', 'Melville, Hermann')) 
"""
from gutenberg.acquire import load_etext
from gutenberg.cleanup import strip_headers

with open('first10000.txt', 'r') as booksfile:
    errors = []
    for book in booksfile:
        print(book.strip())
        book = book.strip()
        try:
            text = strip_headers(load_etext(int(book))).strip()
            # print(text)
            with open('/newvolume/ebooks/{0}.txt'.format(book), 'w') as bookfile:
                bookfile.write(text)
        except Exception as e:
            errors.append(book)
            print(e)

with open('errors.txt', 'w') as errorsfile:
    errorsfile.write('\n'.join(errors))
