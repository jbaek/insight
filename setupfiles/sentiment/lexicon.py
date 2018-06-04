

def main():
    files = ['negative', 'positive']
    for f in files:
        inputfilename = '../txt/corpus/{0}-words.txt'.format(f)
        with open('../txt/corpus/lexicon.txt', 'a') as outputfile:
            with open(inputfilename, 'r', encoding = "ISO-8859-1") as inputfile:
                for line in inputfile:
                    outputfile.write('{0},{1}\n'.format(line.strip(), f))

if __name__ == '__main__':
    main()
