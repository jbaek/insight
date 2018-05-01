from gutenberg.acquire import load_etext
from gutenberg.cleanup import strip_headers

def main():
    with open('first10000.txt', 'r') as booksfile:
        errors = []
        for booknum in booksfile:
            print(book.strip())
            booknum = book.strip()
            try:
                text = strip_headers(load_etext(int(book))).strip()
                with open('/newvolume/ebooks/{0}.txt'.format(book), 'w') as bookfile:
                    bookfile.write(text)
            except Exception as e:
                errors.append(book)
                print(e)

    with open('errors.txt', 'w') as errorsfile:
        errorsfile.write('\n'.join(errors))

if __name__ == '__main__':
    main()
