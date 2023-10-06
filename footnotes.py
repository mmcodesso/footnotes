import concurrent.futures
import spacy
import pandas as pd
import os
from sqlalchemy import create_engine, text
from textacy.extract.basics import ngrams
from glob import glob
from tqdm import tqdm
from collections import Counter

#Set the connection to the database
user = 'footnote'
password = 'footnote'
host = '192.168.50.153'
dbname = 'footnotes'
#engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}/{dbname}"
#                    .format(user = user,password = password,host = host,dbname = dbname))
engine = create_engine("postgresql://{user}:{password}@{host}/{dbname}"
                    .format(user = user,password = password,host = host,dbname = dbname))
conn = engine.connect()


#Load the spacy model
nlp = spacy.load("en_core_web_sm")
#Increase the max length of the text to process
nlp.max_length = 100000000

#Set the path to the dictionaries
#Create a function to load the dictionaries
def load_dictionary(path= './dictionaries'):
    with open(path) as f:
        dictionary = f.readlines()
    dictionary = {word.strip() for word in dictionary}
    return dictionary

#Create a function to get the dictionaries
def get_dictionaries(path):
    path = os.path.join(path,'*.txt')
    files = glob(path)  
    dictionaries = dict()
    for file in files:
        words = load_dictionary(file)
        dictionary_name = os.path.basename(file).split('.')[0]
        dictionaries[dictionary_name] = words       
    return dictionaries

#Create a function to get the min and max ngram
def get_max_ngram(dictionaries):
    listoofwords = list()
    for dictionary_name, dictionary in dictionaries.items():
        for word in dictionary:
            listoofwords.append(len(word.split()))
    max_num = 0
    for i in listoofwords:
        if i > max_num:
            max_num = i
    return tuple(range(1,max_num+1)) 

#Create a function to return the list of files to process
def return_footnotes_id(conn):
    query = text("""
    Select textblock_id from fn32_06232023_10kq_cik
    where not exists (Select 1 from footnotes where footnotes.textblock_id = fn32_06232023_10kq_cik.textblock_id)
    limit 10000
    """)
    
    df = pd.read_sql(query,conn)
    return df.textblock_id.tolist()

#Create a function to return the footnotes by id
def return_footnotes_by_id(id, conn):
    query = text("""
    SELECT
        readable_text
    FROM
        fn32_06232023_10kq_cik
    WHERE
        textblock_id = :id
    """)
    df = pd.read_sql(query,conn, params={'id': id})
    return df.to_dict('records')[0]

#Create a function to parse the text to a corpus     
def parse_text_to_corpus(text):
    return nlp(text, disable=['ner', 'entity_linker', 'textcat', 'entitry_ruler'])


#Create a function to count the number of tokens, the number of words, sentences, stopwords, unique words
def count_words(corpus):
    total_tokens = 0
    counter_stop = 0
    counter_words = 0
    counter_sents = len(list(corpus.sents))
    list_unique = Counter([word.text.lower() for word in corpus if word.is_alpha]).most_common()
    counter_unique = len(list_unique)

    for word in corpus:
        if not word.is_punct and not word.is_currency and not word.is_space:
            total_tokens +=1 
        if word.is_stop:
            counter_stop +=1             
        if word.is_alpha:
            counter_words +=1   
    return  total_tokens, counter_sents, counter_words, counter_stop,counter_unique,list_unique

    
#Create a function to count the number of words in a dictionary
def count_dictionary(corpus, dictionary):
    counter = 0
    conter_by_words = dict()
    for word in corpus:
        if word.lower() in dictionary:
            counter += 1
            conter_by_words[word] = conter_by_words.get(word,0) + 1
    return counter, conter_by_words


#load the global variables
dictionaries_path = './dictionaries'
dictionaries = get_dictionaries(dictionaries_path)
range_ngram = get_max_ngram(dictionaries)


#Create a function to process the footnotes by id
def process_footnotes_by_id(id, dictionaries=dictionaries, conn = conn):

    readable_text = return_footnotes_by_id(id=id, conn=conn)
    corpus = parse_text_to_corpus(readable_text['readable_text'])
    total_tokens, counter_sents, counter_words, counter_stop, counter_unique, list_unique = count_words(corpus)
    
    footnote = dict()
    footnote['textblock_id'] = id
    footnote['total_tokens'] = total_tokens
    footnote['number_sentences'] = counter_sents
    footnote['number_words'] = counter_words
    footnote['number_stopwords'] = counter_stop
    footnote['number_uniquewords'] = counter_unique

    dictionary_words = dict()
    for sentence in corpus.sents:       
        sentence_ngram = ngrams(sentence,range_ngram)    
        sentence_ngram = [str(ngram.text).lower() for ngram in sentence_ngram]

        for dictionary_name, dictionary in dictionaries.items():
            word_counter = 0
            sent_counter = 0
            word_counter, conter_by_words = count_dictionary(sentence_ngram, dictionary)
           
            if word_counter > 0:
                sent_counter = 1

                for word, value in conter_by_words.items():
                    dictionary_words[(id,dictionary_name,word)] = dictionary_words.get((id,dictionary_name,word),0) + value

            footnote[dictionary_name] = footnote.get(dictionary_name,0) + word_counter
            footnote[dictionary_name + '_sentences'] = footnote.get(dictionary_name + '_sentences',0) + sent_counter

    #export table with the unique words
    unique_words = [{'textblock_id': id, 'dictionary_name': 'unique_words', 'word': item[0], 'frequency': item[1],  'term_length': len(item[0].split())} for item in list_unique]
    pd.DataFrame(unique_words).to_sql('footnotes_frequency', conn, if_exists='append', index=False)

    #export table with the dictionary words
    dictionary_word_count = [{'textblock_id': word[0], 'dictionary_name': word[1], 'word': word[2], 'frequency': value, 'term_length': len(word[2].split())} for word, value in dictionary_words.items()]
    pd.DataFrame(dictionary_word_count).to_sql('footnotes_frequency', conn, if_exists='append', index=False)

    #export table with the footnotes
    pd.DataFrame(footnote, index=[0]).to_sql('footnotes', conn, if_exists='append', index=False)
    
    #commit the changes
    conn.commit()
    return

#Create a function to process multicore
def process_footnotes_by_id_multicore(footnotes_id,max_workers = os.cpu_count()):
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(tqdm(executor.map(process_footnotes_by_id, footnotes_id), total=len(footnotes_id), desc='Processing footnotes', unit='files'))
    return results


if __name__ == "__main__":
    #Get the footnotes id
    footnotes_id = return_footnotes_id(conn=conn)
    
    #Process the footnotes
    process_footnotes_by_id_multicore(footnotes_id)