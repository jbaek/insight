import argparse
from nltk.corpus import cmudict

def count_syllables_word(word):
    vowels = "aeiouy"
    numVowels = 0
    lastWasVowel = False
    for wc in word:
        foundVowel = False
        for v in vowels:
            if v == wc:
                if not lastWasVowel:
                    numVowels+=1   #don't count diphthongs
                foundVowel = lastWasVowel = True
                break
        if not foundVowel:  #If full cycle and no vowel found, set lastWasVowel to false
            lastWasVowel = False
    if len(word) > 2 and word[-2:] == "es": #Remove es - it's "usually" silent (?)
        numVowels-=1
    elif len(word) > 1 and word[-1:] == "e":    #remove silent e
        numVowels-=1
    # multiSyllables = 0
    # if numVowels > 1:
        # multiSyllables = 1
    return numVowels

def compare_cmudict_syllables():
    d = cmudict.dict()
    # d = {k: d[k] for k in d.keys() if k[:1] == 'a'}
    diffs = {}
    net_diff = 0
    error_count = 0
    for word, value in d.items():
        syllables = [y for y in value[0] if y[-1].isdigit()]
        cmu_syllables = len(syllables)
        udf_syllables = count_syllables_word(word)
        diffs[word] = (cmu_syllables, udf_syllables)
        net_diff += udf_syllables - cmu_syllables
        if (cmu_syllables == 1 and udf_syllables != 1) or (
                cmu_syllables != 1 and udf_syllables == 1):
            error_count += 1

    print(net_diff)
    print(len(diffs))
    print(float(net_diff)/len(diffs))
    print(float(error_count)/len(diffs))
    return diffs

def parse_arguments():
    """ Parses command line arguments passed from shell script
    :returns: parser object
    """
    parser = argparse.ArgumentParser(description='Process data with spark-nlp')
    parser.add_argument(
            '-w', '--word',
            action='store',
            default='engineering',
            help='number of rows to run through batch processing'
            )
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = parse_arguments()
    word = args.word
    # num_vowels = count_syllables_word(word)
    # print("{0} has {1} vowels".format(word, num_vowels))
    diffs = compare_cmudict_syllables()
    # d = {k: diffs[k] for k in diffs.keys() if k[2] == 'b'}
