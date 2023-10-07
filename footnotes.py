import concurrent.futures
import os
from glob import glob
from collections import Counter

from sqlalchemy import create_engine, text
from textacy.extract.basics import ngrams
from tqdm import tqdm
import pandas as pd
import spacy

#Set the connection to the database
USER = 'footnote'
PASSWORD = 'footnote'
HOST = '192.168.50.153'
DBNAME = 'footnotes'
#engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}/{dbname}"
#                    .format(user = user,password = password,host = host,dbname = dbname))
engine = create_engine("postgresql://{user}:{password}@{host}/{dbname}"
                    .format(user = USER,password = PASSWORD,host = HOST,dbname = DBNAME))
conn = engine.connect()


#Load the spacy model
nlp = spacy.load("en_core_web_sm")
#Increase the max length of the text to process
nlp.max_length = 100000000

#Set the path to the dictionaries
#Create a function to load the dictionaries
def load_dictionary(file_path= './dictionaries/dictionary.txt'):
    """
    Load a dictionary from a file.

    Args:
        file_path (str): The path to the file.

    Returns:
        set: A set of words in the dictionary.
    """
    with open(file_path, encoding='utf-8') as file:
        dictionary = file.readlines()
    dictionary = {word.strip() for word in dictionary}
    return dictionary

#Create a function to get the dictionaries
def get_dictionaries(path):
    """
    Load multiple dictionaries from a directory.

    Args:
        path (str): The path to the directory containing the dictionaries.

    Returns:
        dict: A dictionary containing the loaded dictionaries.
    """
    path = os.path.join(path,'*.txt')
    files = glob(path)
    dict_list = {}
    for file in files:
        words = load_dictionary(file)
        dict_name = os.path.basename(file).split('.')[0]
        dict_list[dict_name] = words
    return dict_list

#Create a function to get the min and max ngram
def get_max_ngram(list_dictionaries):
    """
    Get the maximum ngram from a list of dictionaries.

    Args:
        list_dictionaries (dict): A dictionary containing the loaded dictionaries.

    Returns:
        tuple: A tuple containing the range of ngrams.
    """
    list_of_words = []
    for dictionary in list_dictionaries.values():
        for word in dictionary:
            list_of_words.append(len(word.split()))
    max_num = 0
    for i in list_of_words:
        if i > max_num:
            max_num = i
    return tuple(range(1, max_num + 1))

#Create a function to return the list of files to process
def return_footnotes_id(connection):
    """
    Returns a list of textblock_ids from the fn32_06232023_10kq_cik table that
     do not exist in the footnotes table.

    Args:
        connection (sqlalchemy.engine.base.Connection): The database connection object.

    Returns:
        list: A list of textblock_ids.
    """
    query = text("""
    Select textblock_id from fn32_06232023_10kq_cik
    where not exists (Select 1 from footnotes where
        footnotes.textblock_id = fn32_06232023_10kq_cik.textblock_id)
        limit 1000
    """)
    df_footnotes = pd.read_sql(query, connection)
    return df_footnotes.textblock_id.tolist()

#Create a function to return the footnotes by id
def return_footnotes_by_id(footnote_id, connection):
    """
    Returns the readable text of a footnote given its ID.

    Args:
        footnote_id (int): The ID of the footnote to retrieve.
        connection (sqlalchemy.engine.base.Connection): The database connection object.

    Returns:
        dict: A dictionary containing the readable text of the footnote.
    """
    query = text("""
    SELECT
        readable_text
    FROM
        fn32_06232023_10kq_cik
    WHERE
        textblock_id = :id
    """)
    df_footnotes = pd.read_sql(query, connection, params={'id': footnote_id})
    return df_footnotes.to_dict('records')[0]

#Create a function to parse the text to a corpus
def parse_text_to_corpus(text_corpus):
    """
    Parses the given text into a spaCy corpus.

    Args:
        text_corpus (str): The text to parse.

    Returns:
        spacy.tokens.doc.Doc: The parsed spaCy corpus.
    """
    return nlp(text_corpus, disable=['ner', 'entity_linker', 'textcat', 'entitry_ruler'])


#Create a function to count the number of tokens, the number of words, sentences, stopwords, unique words
def count_words(corpus):
    """
    Count the number of tokens, words, sentences, stopwords, unique words in a given corpus.

    Args:
    corpus (spacy.tokens.doc.Doc): The corpus to be processed.

    Returns:
    tuple: A tuple containing the total number of tokens, number of sentences, 
    number of words, number of stopwords, number of unique words, and a list of 
    the most common unique words.
    """
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
    """
    Returns the count of words in the given corpus that are present in the given dictionary,
    along with a dictionary containing the count of each word in the corpus that is present in the dictionary.

    Args:
    - corpus (list): A list of words to be searched in the dictionary.
    - dictionary (set): A set of words to be searched in the corpus.

    Returns:
    - A tuple containing two elements:
        - The count of words in the corpus that are present in the dictionary.
        - A dictionary containing the count of each word in the corpus that is present in the dictionary.
    """
    counter = 0
    conter_by_words = dict()
    for word in corpus:
        if word.lower() in dictionary:
            counter += 1
            conter_by_words[word] = conter_by_words.get(word,0) + 1
    return counter, conter_by_words


#load the global variables
DICTIONARIES_PATH = './dictionaries'
dictionaries = get_dictionaries(DICTIONARIES_PATH)
range_ngram = get_max_ngram(dictionaries)


#Create a function to process the footnotes by id
def process_footnotes_by_id(id_, list_dictionaries=dictionaries, connection = conn):
    """
    Processes footnotes for a given ID by counting the number of tokens, sentences, words, 
    stopwords, unique words, and words from a given list of dictionaries.
    The function then exports the results to three separate tables in a 
    database: footnotes_frequency, footnotes, and footnotes_frequency.
    
    Args:
    - id_ (int): The ID of the footnote to be processed.
    - list_dictionaries (dict): A dictionary containing the names and words 
      of the dictionaries to be used for counting.
    - connection (sqlite3.Connection): A connection object to the database.
    
    Returns:
    - None
    """
    readable_text = return_footnotes_by_id(footnote_id=id_, connection=connection)
    corpus = parse_text_to_corpus(readable_text['readable_text'])
    total_tokens, counter_sents, counter_words, counter_stop, counter_unique, list_unique = count_words(corpus)
    
    footnote = dict()
    footnote['textblock_id'] = id_
    footnote['total_tokens'] = total_tokens
    footnote['number_sentences'] = counter_sents
    footnote['number_words'] = counter_words
    footnote['number_stopwords'] = counter_stop
    footnote['number_uniquewords'] = counter_unique

    dictionary_words = dict()
    for sentence in corpus.sents:       
        sentence_ngram = ngrams(sentence,range_ngram)    
        sentence_ngram = [str(ngram.text).lower() for ngram in sentence_ngram]

        for dictionary_name, dictionary in list_dictionaries.items():
            word_counter = 0
            sent_counter = 0
            word_counter, conter_by_words = count_dictionary(sentence_ngram, dictionary)
            if word_counter > 0:
                sent_counter = 1
                for word, value in conter_by_words.items():
                    dictionary_words[(id_,dictionary_name,word)] = dictionary_words.get((id_,dictionary_name,word),0) + value

            footnote[dictionary_name] = footnote.get(dictionary_name,0) + word_counter
            footnote[dictionary_name + '_sentences'] = footnote.get(dictionary_name + '_sentences',0) + sent_counter

    #export table with the unique words
    unique_words = [{'textblock_id': id_, 'dictionary_name': 'unique_words', 'word': item[0], 
                     'frequency': item[1],  'term_length': len(item[0].split())} for item in list_unique]
    pd.DataFrame(unique_words).to_sql('footnotes_frequency', connection, if_exists='append', index=False)

    #export table with the dictionary words
    dictionary_word_count = [{'textblock_id': word[0], 'dictionary_name': word[1], 'word': word[2], 
                              'frequency': value, 'term_length': len(word[2].split())} for word, value in dictionary_words.items()]
    pd.DataFrame(dictionary_word_count).to_sql('footnotes_frequency', connection, if_exists='append', index=False)

    #export table with the footnotes
    pd.DataFrame(footnote, index=[0]).to_sql('footnotes', connection, if_exists='append', index=False)
    
    #commit the changes
    connection.commit()
    return

#Create a function to process multicore
def process_footnotes_by_id_multicore(list_footnotes_id, max_workers = os.cpu_count()):
    """
    Process footnotes by ID using multiple processes.

    Args:
        list_footnotes_id (list): A list of footnote IDs to process.
        max_workers (int): The maximum number of worker processes to use. Defaults to the number of CPUs on the system.

    Returns:
        list: A list of results from processing the footnotes.
    """
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(tqdm(executor.map(process_footnotes_by_id, list_footnotes_id),
                             total=len(list_footnotes_id), desc='Processing footnotes', unit='files'))
    return results


if __name__ == "__main__":
    #Get the footnotes id
    footnotes_id = return_footnotes_id(connection=conn)

    #Process the footnotes
    process_footnotes_by_id_multicore(footnotes_id)
