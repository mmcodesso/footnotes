import csv
import math
import os
import re
import shutil
import sys

from nltk.corpus import cmudict

sys.path.append(os.path.join(os.path.dirname(sys.path[0]), 'utils'))


'''
loading nltk dictionary to calculate complex_words globally 
'''
d = cmudict.dict() 
vowels = "aeiouy"

def count_words(passage):
    return len(re.sub("[^\w]", " ",  passage).split())

def count_sentences(passage):
    return len(re.split(r'[.!?]+', passage))-1 

def count_syllables(passage):
    no_of_syllables = 0
    word_list = re.sub("[^\w]", " ",  passage).split()
    for word in word_list:
        no_of_syllables = no_of_syllables + syllable_count(word)
    return no_of_syllables

'''
Manually count the number of syllables in a word 
w/o using the nltk cmu dict
Ref:https://stackoverflow.com/questions/46759492/syllable-count-in-python
'''
def manual_syllable_count(word):
    word = word.lower()
    count = 0
    if word[0] in vowels:
        count += 1
    for index in range(1, len(word)):
        if word[index] in vowels and word[index - 1] not in vowels:
            count += 1
            if word.endswith("e"):
                count -= 1
    if count == 0:
        count += 1
    return count

'''
Calculate sentiment of the list_of_words in the given 
passage
'''
def sentiment(passage,list_of_sentiment_words):
    passage = remove_LM_stop_words(passage)
    # f=open('passage1.txt','w')
    # f.write(str(passage))
    # f.close()
    list_of_sentiment_words_in_passage = ""
    no_of_words = len(passage)
    #print("no of words "+ str(no_of_words))
    if(no_of_words!=0):
        #print("check 1 passed")
        no_of_sentiment_words = 0
        for word in passage.split():
            #print(word)
            #print("\n")
            if word.lower() in list_of_sentiment_words:
                #print("check 2 passed")
                no_of_sentiment_words= no_of_sentiment_words+1
                list_of_sentiment_words_in_passage=list_of_sentiment_words_in_passage+word.lower() + " "
        sentiment = no_of_sentiment_words/no_of_words
        return sentiment
    else:
        return 0

'''
Find the words associated with the particular sentiment 
in the passage
'''
def sentiment_words(passage,list_of_sentiment_words):
    passage = remove_LM_stop_words(passage)
    list_of_sentiment_words_in_passage = ""
    for word in passage.split():
        if word.lower() in list_of_sentiment_words:
            list_of_sentiment_words_in_passage=list_of_sentiment_words_in_passage+word.lower() + " "
    #print("\n")
    #print(list_of_sentiment_words_in_passage)
    return list_of_sentiment_words_in_passage
'''
Removes LM stop words from passage
'''
def remove_LM_stop_words(passage):
    passage = re.sub("[^\w]", " ",  passage).split()
    auditor = read_stop_words('StopWords_Auditor')
    numbers = read_stop_words('StopWords_DatesandNumbers')
    generic= read_stop_words('StopWords_Generic')
    generic_long= read_stop_words('StopWords_GenericLong')
    geographic= read_stop_words('StopWords_Geographic')
    names= read_stop_words('StopWords_Names')

    passage=[word for word in passage if word.lower() not in auditor]
    passage=[word for word in passage if word.lower() not in numbers]
    passage=[word for word in passage if word.lower() not in generic]
    passage=[word for word in passage if word.lower() not in generic_long]
    passage=[word for word in passage if word.lower() not in geographic]
    passage=[word for word in passage if word.lower() not in names]
    return ' '.join(word for word in passage)

'''
Remove nltk stop words from passage
Ref:https://stackoverflow.com/questions/19560498/faster-way-to-remove-stop-words-in-python
'''
def remove_nltk_stop_words(passage):
    from nltk.corpus import stopwords
    cachedStopWords = stopwords.words("english")
    passage = ' '.join([word for word in passage.split() if word not in cachedStopWords])
    return passage
'''
Returns the total number of characters (alpha)
excluding the whitespace,numbers,punctuation
'''
def count_characters(passage):
    passage = [c.lower() for c in passage if c.isalpha()]
    return len(passage)

'''
returns the count of words greater than n or more characters in length 
in the passage
'''
def manual_no_of_n_letter_words(passage,n):
    word_list = re.sub("[^\w]", " ",  passage).split()
    return len(list(filter(lambda word: len(word)>=n, word_list)))


def manual_no_of_complex_words(passage):
    no_of_complex_words=0
    word_list = re.sub("[^\w]", " ",  passage).split()
    i=0
    for word in word_list:
        if (syllable_count(word)>=3):
            no_of_complex_words = no_of_complex_words+1
    return no_of_complex_words

def gunning_fog(passage):
    no_of_words = count_words(passage)
    no_of_sentences = count_sentences(passage)
    no_of_complex_words=0
    word_list = re.sub("[^\w]", " ",  passage).split()
    i=0
    for word in word_list:
        if (syllable_count(word)>=3):
            no_of_complex_words = no_of_complex_words+1
    return 0.4*((no_of_words/no_of_sentences)+100*(
        no_of_complex_words/no_of_words))

def flesch_reading_ease(passage):
    no_of_words = count_words(passage)
    no_of_sentences = count_sentences(passage)
    no_of_syllables = count_syllables(passage)
    return 206.835-1.015*(no_of_words/no_of_sentences)-84.6*(
        no_of_syllables/no_of_words)

def flesch_kincaid_grade_level(passage):
    no_of_words = count_words(passage)
    no_of_sentences = count_sentences(passage)
    no_of_syllables = count_syllables(passage)
    return 0.39*(no_of_words/no_of_sentences)+11.8*(
        no_of_syllables/no_of_words)-15.59

def RIX(passage):
    no_of_seven_or_more_letter_words=manual_no_of_n_letter_words(passage,7)
    no_of_sentences = count_sentences(passage)
    return no_of_seven_or_more_letter_words/no_of_sentences

'''
should it be rounded up? 
'''
def coleman_liau_index(passage):
    no_of_sentences = count_sentences(passage)
    no_of_words = count_words(passage)
    no_of_characters = count_characters(passage)
    return 5.88*(no_of_characters/no_of_words)-29.6*(no_of_sentences/
        no_of_words)-15.8

def smog(passage):
    no_of_complex_words = manual_no_of_complex_words(passage)
    no_of_sentences = count_sentences(passage)
    return 1.043 * math.sqrt(no_of_complex_words * 
        30/no_of_sentences)+3.1291

def LIX(passage):
    no_of_words = count_words(passage)
    no_of_sentences = count_sentences(passage)
    no_of_six_or_more_letter_words = manual_no_of_n_letter_words(passage,6)
    return no_of_words/no_of_sentences + (no_of_six_or_more_letter_words * 
        100)/no_of_words

'''
should it be rounded up? 
'''
def automated_readability_index(passage):
    no_of_sentences = count_sentences(passage)
    no_of_words = count_words(passage)
    no_of_characters = count_characters(passage)
    return 4.71*(no_of_characters/no_of_words)+0.5*(no_of_words/
        no_of_sentences)-21.43

'''
ref: https://stackoverflow.com/questions/5087493/to-find-the-number-of-syllables-in-a-word
'''
def syllable_count(word):
    try:
        no_of_syllables = [len(list(y for y in x if y[-1].isdigit())) for x in d[word.lower()]]
        return no_of_syllables[0]
    except Exception as e:
        return manual_syllable_count(word)
        

def list_to_csv(lists, document_ids, output_file):

    batchsize = 10
    headers = [["index1","index2","similarity"]]

    if os.path.isfile(output_file):
        os.remove(output_file)

    with open(output_file, "w") as sim_pair_file:

        writer = csv.writer(sim_pair_file)
        for slider_index in range(0, len(lists), batchsize):
            begin_index = slider_index
            end_index = slider_index + batchsize

            similarities_for_range = lists[begin_index: end_index]

            writer.writerows(similarities_for_range)




def create_date_folder(foldername):
    """
    """
    if(os.path.exists(foldername)):
        shutil.rmtree(foldername)
    os.makedirs(foldername)

    print("New directories created")






def matrix_to_pairwise_csv(matrix, document_ids, output_file):
    """

    Arguments:
    - `matrix`:
    """

    batchsize = 100
    headers = [["index1","index2","similarity"]]

    if os.path.isfile(output_file):
        os.remove(output_file)

    with open(output_file, "w") as sim_pair_file:

        writer = csv.writer(sim_pair_file, lineterminator="\n")

        for slider_index in range(0, len(matrix), batchsize):


            begin_index = slider_index

            if begin_index + batchsize > len(document_ids):
                batchsize = len(document_ids) - begin_index

            similarities_for_range = []

            for i in range(batchsize):
                for j in range(len(matrix)):
                    first_document_index = document_ids[begin_index + i]
                    second_document_index = document_ids[j]
                    similarity = matrix[begin_index + i, j]

                    similarities_for_range.append([first_document_index, second_document_index, similarity])

            writer.writerows(similarities_for_range)




def output_to_csv(_type, output, document_ids, output_file):
    if _type == "scikit":
        return matrix_to_pairwise_csv(output, document_ids, output_file)
    elif _type == "word2vec":
        return list_to_csv(output, document_ids, output_file)

def reaarrage_df_columns(data_frame):
    cols = data_frame.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    data_frame = data_frame[cols]
    return data_frame
