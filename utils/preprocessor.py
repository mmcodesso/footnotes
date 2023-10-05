import re
import signal
import string
from bs4 import BeautifulSoup
import subprocess
import string
import os
from nltk.stem.snowball import SnowballStemmer
import pandas as pd
import gc
import tqdm
import spacy
import re
from tqdm import tqdm
from bs4 import BeautifulSoup
import numpy as np
from sklearn.feature_extraction import text

__author__ = "Sreejith Sreekumar", "Prasanth Murali", "Shreyans Jasoriya", "Jai Soni","Sai Garimella"
__email__ = "sreekumar.s@husky.neu.edu", "murali.pr@husky.neu.edu", "jasoriya.s@husky.neu.edu", "soni.j@husky.neu.edu"\
            ,"garimella.s@husky.neu.edu"
__version__ = "0.0.3"

null = 0
empty = 0

foo = []
detectNumbersInText = r'[\d.,]*\d+'
detectTitleNumber = r"\d+\.\s"
regex = r"(?i)[iI]n\s\d+,\s\d+\sand\s\d+|\d+[\/:\-]\d+[\/:\-\s]*[\dAaPpMm]*|\w+\s\d+[\,]\s\d+|due\s\d+|((January|February|March|April|May|June|July|August|September|October|November|December)\s*\d+\s*\d+)|\d+\sNotes|((January|February|March|April|May|June|July|August|September|October|November|December)\s*\d+)|\d+ and \d+ Notes|during the \d+|[Ii]n\s\d+\sand\s\d+|[Ii]n\s\d+|[dD]uring\s\d+|(19|[2][0-9])\d{2}|[Nn]ote[s]* \d+[,and\s\d+]*[\s!\"#&\'()*+,-./:—;<=>?@[\\\]\^_`{\|}~]|[Ll]evel\s+\d+[\s!\"#&\'()*+,-.\/:—;<=>?@[\\\]\^_`{\|}~]?|[({\[]*\d+[\sto\d\-)}\]]*[Qq]uarter[\ss!\"”#&\'()*+,-./:—;<=>?@[\\\]\^_`{\|}~]*|[({\[]*\d+[\sto\d\-)}\]]*[Mm]onth[\ss!\"”#&\'()*+,-./:—;<=>?@[\\\]\^_`{\|}~]*|[({\[]*\d+[\sto\d\-)}\]]*[Yy]ear[\ss!\"#&\'()*+,-./:—;<=>?@[\\\]\^_`{\|}~]*|[Ss]ection[\ss!\"”#&\'()*+,-./:—;<=>?@[\\\]\^_`{\|}~]*\d+[()[\]{}a-zA-Z\d]*[,and\s\d]+[()[\]{}a-zA-Z\d]*|[({\[]*\d+[\sto\d\-)}\]]*[Dd]ay[\ss!\"”#&\'()*+,-./:—;<=>?@[\\\]\^_`{\|}~]*|([Ff]orm|[Rr]ule) [a-zA-Z\d]+-[a-zA-Z\d]+|401\(k\)|[U.S.|US]+ Patent No. \d+,\d+,\d+[ and,\d+]+|([Cc]ase [Nn]o|Case Number)[.\s]+[a-zA-Z\d#]+[-a-zA-Z\d#]+|[Ii]nternal [Rr]evenue [Cc]ode [Ss]ection[s ]+[,and \d+]+|(ASC|Accounting Standards Codification)[\sTopic]*\d+[\ss!\"#&\'()*+,-./:—;<=>?@[\\\]\^_`{\|}~]*\d+|\(\$\d+s\)|\s\d\)|\s\d\.[^\d]|[Yy]ear[\ss!\"#&\'*+,-./:—;<=>?@[\\\]\^_`{\|}~]*\d+|[Mm]onth[\ss!\"”#&\'*+,-./:—;<=>?@[\\\]\^_`{\|}~]*\d+|[Dd]ay[\ss!\"”#&\'*+,-./:—;<=>?@[\\\]\^_`{\|}~]*\d+|[Qq]uarter[\ss!\"”#&\'*+,-./:—;<=>?@[\\\]\^_`{\|}~]*\d+|\d{1,4} [\w\s]{1,20}(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd)\W?(?=\s|$)|\b\d{5}(?:[-\s]\d{4})?\b|([Cc]hapter|[Tt]opic|SFAS|Unit|docket number|[Cc]iv[.]{0,1})\s+\d+[\s!\"#&\'()*+,-.\/:—;<=>?@[\\\]\^_`{\|}~]*\d*|[a-zA-Z]+\d+|\([1-4]\)|\n[1-3]\n"
# \d+[(){}[\]]*[a-zA-Z]+| ---- Last added regex by shreyans, removed by Jai
# |\([1-4]\)|\n[1-3]\n ---- Added by Jai
removeLeftoverTags = r"""[ !\"#$%&\'()*+,-.\/:;<=>?@[\\\]\^_`{\|}~\w]*\">"""
remove_superscript_tag = r"<sup>.+?</sup>"


def lemmatize(s, parser):
    doc = parser(s)
    return " ".join([token.lemma_ for token in doc if token.lemma_ != '-PRON-'])


def snow_ball_stem(passage):
    stemmer = SnowballStemmer("english")
    return " ".join(str(x) for x in ([stemmer.stem(word) for word in passage.split(" ")]))


def remove_character(passage, char):
    return re.sub(r'e\b', '', passage)


def remove_dollar_keep_only_alpha_sentences(passage):
    passage = passage.replace("$", '')
    passage = passage.split('.')
    processed_passage = ''
    for i in range(0, len(passage)):
        isAlpha = re.search('[a-zA-Z]', passage[i])
        if (isAlpha != None):
            processed_passage = processed_passage + '.' + passage[i]
    processed_passage = processed_passage.replace(' .', '.')
    processed_passage = processed_passage.replace('. ', '.')
    processed_passage = processed_passage[1:]
    '''
    if last character in the passage is not a period, include it
    '''
    if (len(processed_passage) != 0 and processed_passage[-1] != '.'):
        processed_passage = processed_passage + '.'
    return processed_passage


'''
Add space in between every number immediately followed by a letter
and every letter immediately followed by a number
Partial Ref:https://stackoverflow.com/questions/20003025/find-1-letter-and-2-numbers-using-regex
'''


def space_after_number_letter(passage):
    pattern1 = "[a-zA-Z]\d"
    pattern2 = "\d[a-zA-Z]"
    list_of_letter_and_number = re.findall(pattern1, passage)
    for case in list_of_letter_and_number:
        passage = passage.replace(case, case[0] + " " + case[1])
    list_of_number_and_letter = re.findall(pattern2, passage)
    for case in list_of_number_and_letter:
        passage = passage.replace(case, case[0] + " " + case[1])
    return passage


'''
Add space after every period in the passage if it doesn't exist
Ref:https://stackoverflow.com/questions/29506718/having-trouble-adding-a-space-after-a-period-in-a-python-string
'''


def space_after_period(passage):
    '''
    passage = passage.replace('. ','.')
    return passage
    '''
    return re.sub(r'\.(?! )', '. ', passage)


def replace_null_with_empty_string(html):
    '''
      Replace null cells in "value" with an empty string
      and creates a corresponding new string to put in
      "cleaned_value" column
    '''

    global null
    global empty
    if html is not None:
        if html.strip() == "":
            print("Empty string")
            empty += 1
            return ""
    else:
        # print("Null")
        null += 1
        return ""
    return html


def remove_table(clean_text):
    while (True):
        try:
            start_idx = clean_text.index("<table")
            end_idx = clean_text.index("</table>")
            '''
            remove tables where numeric/numeric+alpha > 0.1
            '''
            is_remove_table = should_remove_table(clean_text[start_idx + len("<table>"):end_idx])
            if (is_remove_table):
                clean_text = clean_text[0:start_idx] + clean_text[end_idx + len("</table>"):len(clean_text)]
            else:
                clean_text = clean_text[0:start_idx] + clean_text[start_idx + len("<table>"):end_idx] + clean_text[
                                                                                                        end_idx + len(
                                                                                                            "</table>"):len(
                                                                                                            clean_text)]
        except:
            return clean_text


def should_remove_table(clean_text):
    numeric = sum(char.isdigit() for char in clean_text)
    alphabet = sum(char.isalpha() for char in clean_text)
    return numeric / (alphabet + numeric) > 0.1


def has_nested_table(part):
    try:
        part.index('<table')
        return True
    except ValueError:
        return False


def has_no_numeric(part):
    #    part = text[start_idx:end_idx]
    soup = BeautifulSoup(part, 'lxml')
    part = soup.text
    numeric = len(re.findall('\d+', part))
    alphabet = len(re.findall('[a-zA-Z]+', part))
    try:
        numToAlpha = numeric / (alphabet + numeric) < 0.1
    except ZeroDivisionError:
        return True
    if numToAlpha:
        return True
    elif bool(re.match(r'\s*Note\s*\d+.\s*[\w+ ]+\s*', part)):
        # removes table tags from title of document
        return True
    elif len(soup.findAll('tr')) <= 2:
        return True
    return False


def remove_table_if_no_numeric(clean_text):
    ind = 0
    i = 1
    text = clean_text
    hasTableStartTags = True
    while (hasTableStartTags):
        #        print(i)
        try:
            start_idx = text.index("<table")
            end_idx = text.index("</table>")
        except ValueError:
            return clean_text
        is_remove_table = has_no_numeric(text[start_idx:end_idx])
        boolean = has_nested_table(text[start_idx + len("<table>"):end_idx])
        if boolean:
            clean_text = clean_text[0:ind + start_idx] + clean_text[ind + start_idx + len("<table>"):len(clean_text)]
            ind = start_idx
            text = clean_text[ind:end_idx + len("</table>")]
        elif is_remove_table:
            clean_text = clean_text[0:ind + start_idx] + clean_text[ind + start_idx + len("<table>"):len(clean_text)]
            #            ind += end_idx+len("</table>")
            ind += end_idx
            text = clean_text[ind:]
        else:
            ind += end_idx + len("</table>")
            text = clean_text[ind:]
        i += 1
        try:
            clean_text[ind:].index('<table')
        except ValueError:
            hasTableStartTags = False
    return clean_text


def remove_nested_tables(clean_text):
    ind = 0
    i = 1
    text = clean_text
    hasTableStartTags = True
    while (hasTableStartTags):
        try:
            start_idx = text.index("<table")
            end_idx = text.index("</table>")
        except ValueError:
            return clean_text
        boolean = has_nested_table(text[start_idx + len("<table>"):end_idx])
        if boolean:
            clean_text = clean_text[0:ind + start_idx] + clean_text[ind + start_idx + len("<table>"):len(clean_text)]
            ind = start_idx
            text = clean_text[ind:end_idx + len("</table>")]
        else:
            ind += end_idx + len("</table>")
            text = clean_text[ind:]
        i += 1
        try:
            clean_text[ind:].index('<table')
        except ValueError:
            hasTableStartTags = False
    return clean_text


def kill_lynx(pid):
    os.kill(pid, signal.SIGKILL)
    os.waitpid(-1, os.WNOHANG)
    print("lynx killed")


def get_text_from_html(x):
    """

    """
    output = ''
    try:
        ps = subprocess.Popen(('echo', x), stdout=subprocess.PIPE)
        output = subprocess.check_output(('lynx', '--dump', '--stdin'), stdin=ps.stdout)
        ps.wait()
    except:
        pass

    return output


def get_readable_text(raw_html):
    """
    Arguments:
    - `x`:
    """
    '''
    raw_html = bytes(raw_html, 'utf-16').decode("utf-16", 'ignore')
    _cleantext = BeautifulSoup(raw_html).text
    '''

    raw_html = bytes(raw_html, 'utf-16').decode("utf-16", 'ignore')
    _cleantext = BeautifulSoup(raw_html, 'lxml')
    for e in _cleantext.findAll('br'):
        e.replace_with(" ")
    _cleantext = _cleantext.getText(separator=u' ')
    paragraphs = [s.strip() for s in _cleantext.splitlines()]
    cleaned_paragraphs = []
    for para in paragraphs:
        cleantext = " ".join(para.split())
        cleantext = ''.join(x for x in cleantext if x in string.printable)
        cleaned_paragraphs.append(cleantext)

    cleantext = "\n".join(cleaned_paragraphs)
    strs = cleantext.replace('\n', ' ')  # replace multiple newlines(EDIT: @kaushalshetty)
    cleantext = re.sub(r' +', " ", strs)  # replace multiple spaces (EDIT: @kaushalshetty)
    return cleantext


def clean_text(text):
    '''
    Parameters:
        ing
    Returns:
        String
    This function does the following process on the text:
        convert to lowercase
        removes punctuation except '-' and '.'(@kaushalshetty)
        removes special characters
    '''
    '''
    New implementation to remove the punctuation and replace with space
    Ref: https://stackoverflow.com/questions/42614458/how-to-replace-punctuation-with-whitespace
    '''
    punc_list = list(string.punctuation.replace('.', '').replace('-', '').replace(',',''))  # except dots and dashes
    translator = text.maketrans(dict.fromkeys(punc_list, ""))
    cleantext = text.lower().translate(translator)
    ## clear off numbers and normalize spaces between words
    ## and lowercase it
    cleantext = " ".join([s for s in cleantext.split(" ") if s.strip() is not ""]).lower()
    ## remove any non-printable (non-ascii) characters in the text
    printable = set(string.printable)
    cleantext = list(filter(lambda x: x in printable, cleantext))
    cleantext = "".join(cleantext)
    ## remove roman numberals from string which
    ## are not in brackets
    toremove = [' ii ', ' iii ', ' iv ', ' v ', ' vi ', ' vii ', ' viii ', ' ix ', ' x ', '!', '@', '#', '$', '%', '^',
                '&', '*', '$.']
    text_array = cleantext.split("\s+")
    cleantext = [word.strip() for word in text_array if word not in toremove]
    cleantext = " ".join(cleantext)

    # numbers = [1]
    # while (len(numbers) != 0):
    #     numbers = re.findall('\d+', cleantext)
    #     for number in numbers:
    #         cleantext = cleantext.replace(number, " ") # lets keep numbers(EDIT: @kaushalshetty)

    cleantext = re.sub(' +', ' ', cleantext)
    return cleantext.strip()


def clean_html_and_extract_text(raw_html):
    '''
       Clean an html string that comes from "cleaned_value"  column
    '''
    #    global foo

    ## use regular expressions to remove roman numberals inside brackets
    ## eg. (iv), (ix) etc.
    raw_html = re.sub('\([v|i|x]+\)', '', raw_html)
    # raw_html = re.sub('\s\d+\s', '', raw_html)

    ## clear off the non ascii characters, remove the html tags
    ## and get just the text from the document
    raw_html = bytes(raw_html, 'utf-16').decode("utf-16", 'ignore')
    _cleantext = BeautifulSoup(raw_html, 'lxml')
    for e in _cleantext.findAll('br'):
        e.replace_with(" ")
    _cleantext = _cleantext.getText(separator=u' ')
    cleantext = _cleantext

    cleantext = " ".join(cleantext.split())
    cleantext = ''.join(x for x in cleantext if x in string.printable)

    # foo.append(cleantext)

    # for checking on various libraries
    # extract_fog_score(cleantext)

    ## clear off punctuations in the text
    '''
    table = cleantext.maketrans("","", string.punctuation)
    cleantext = cleantext.translate(table)
    '''

    '''
    New implementation to remove the punctuation and replace with space
    Ref: https://stackoverflow.com/questions/42614458/how-to-replace-punctuation-with-whitespace
    '''
    punc_list = list(string.punctuation)
    translator = cleantext.maketrans(dict.fromkeys(punc_list, " "))
    cleantext = cleantext.lower().translate(translator)

    ## clear off numbers and normalize spaces between words
    ## and lowercase it
    cleantext = " ".join([text for text in cleantext.split(" ")
                          if text.strip() is not ""]).lower()
    '''
    cleantext = " ".join([text for text in cleantext.split(" ")
                          if text.strip() is not "" and text.isdigit() is False]).lower()
    '''

    ## remove any non-printable (non-ascii) characters in the text
    printable = set(string.printable)
    cleantext = list(filter(lambda x: x in printable, cleantext))
    cleantext = "".join(cleantext)

    ## remove roman numberals from string which
    ## are not in brackets
    toremove = [' ii ', ' iii ', ' iv ', ' v ', ' vi ', ' vii ', ' viii ', ' ix ', ' x ', '!', '@', '#', '$', '%', '^',
                '&', '*', '$.']
    text_array = cleantext.split("\s+")
    cleantext = [word.strip() for word in text_array if word not in toremove]
    cleantext = " ".join(cleantext)

    ## clear off all arabic numerals / digits in the text which are attached
    ## together with text

    numbers = [1]
    while (len(numbers) != 0):
        numbers = re.findall('\d+', cleantext)
        for number in numbers:
            cleantext = cleantext.replace(number, " ")

    cleantext = re.sub(' +', ' ', cleantext)

    return cleantext.strip()

    return fog_index


def extract_count_of_tables(raw_html):
    raw_html = remove_table_if_no_numeric(raw_html)
    try:
        return len(pd.read_html(raw_html))
    except ValueError:
        return 0


def remove_table_head(clean_text):
    while (True):
        try:
            start_idx = clean_text.index("<!-- Begin Table Head -->")
            end_idx = clean_text.index("<!-- End Table Head -->")
            clean_text = clean_text[0:start_idx] + clean_text[end_idx + len("<!-- End Table Head -->"):len(clean_text)]
        except:
            return clean_text


def remove_consecutive_dates_from_list(list_of_values):
    res = []
    date_year_list = [str(x) for x in np.arange(2000, 2020).tolist() + [30, 31]]
    for i, v in enumerate(list_of_values):
        if v in date_year_list or v.split('.')[0] in date_year_list:
            if i == 0 or v != list_of_values[i - 1]:
                if i == 0 or v.split('.')[0] != list_of_values[i - 1]:
                    res.append(v.split('.')[0])
        else:
            res.append(v)
    return res


def extract_count_of_num_in_tables(raw_html):
    raw_html = remove_table_if_no_numeric(raw_html)
    date_year_list = np.arange(2000, 2020).tolist() + [31] + [30]
    try:
        soup = BeautifulSoup(raw_html, 'lxml')
        all_tables = soup.find_all('table')
        numbers_in_table = []
        for table in all_tables:
            numbers_in_table.append(
                len(remove_consecutive_dates_from_list(re.findall(detectNumbersInText, table.get_text()))))
        return sum(numbers_in_table)
    except ValueError:
        return 0


def is_number(s):
    try:
        int(s)
        return True
    except ValueError:
        try:
            float(s)
            return True
        except ValueError:
            return False
        return False


def remove_all_tables(clean_text):
    while (True):
        try:
            start_idx = clean_text.index("<table")
            end_idx = clean_text[start_idx:].index("</table>")
            clean_text = clean_text[0:start_idx] + clean_text[start_idx + end_idx + len("</table>"):len(clean_text)]
        except:
            return clean_text


def extract_count_of_num_not_in_tables(raw_html):
    raw_html = remove_table_if_no_numeric(raw_html)
    textWithoutTable = remove_all_tables(raw_html)
    #    cleanText = get_readable_text(textWithoutTable)
    cleanText = None
    if cleanText is None:
        soup = BeautifulSoup(textWithoutTable, 'lxml')
        cleanText = soup.text
        cleanText = re.sub(removeLeftoverTags, '', cleanText)
    return len(re.findall(detectNumbersInText, cleanText))


def preprocessed_count_of_num_in_tables_regex(raw_html):
    gc.collect()
    try:
        raw_html = remove_table_if_no_numeric(raw_html)
        soup = BeautifulSoup(raw_html, 'lxml')
        all_tables = soup.find_all('table')
        numbers_in_table = []
        for table in all_tables:
            result = re.sub(regex, '', table.get_text())
            numbers_in_table.append(len(re.findall(detectNumbersInText, result)))
        return sum(numbers_in_table)
    except ValueError:
        return 0


def preprocessed_count_of_num_not_in_tables_regex(raw_html):
    gc.collect()
    raw_html = remove_table_if_no_numeric(raw_html)
    textWithoutTable = remove_all_tables(raw_html)
    textWithoutTable = re.sub(r'>\s*[\s\"\'-\/:;=>\w%]*\">', '>', textWithoutTable)
    textWithoutTable = re.sub(remove_superscript_tag, '', textWithoutTable)
    cleanText = get_readable_text(textWithoutTable)
    if cleanText is None:
        soup = BeautifulSoup(textWithoutTable, 'lxml')
        cleanText = soup.text
    cleanText = re.sub(removeLeftoverTags, '', cleanText)
    result = re.sub(regex, '', cleanText)
    result = re.sub(r"\(\d\)", '', result)
    result = re.sub(detectTitleNumber, '', result, count=1)
    return len(re.findall(detectNumbersInText, result))


def preprocess_column(df_data, column_name, do_lemmatize=True, no_stopwords=True):
    tqdm.pandas()
    df_data = df_data[pd.notnull(df_data[column_name])]
    df_data['temp'] = df_data[column_name].progress_apply(
        lambda x: remove_all_tables(x))
    df_data['readable_text'] = df_data['temp'].progress_apply(lambda x: get_readable_text(x))
    df_data.drop(['temp'], axis=1, inplace=True)
    df_data['processed_value'] = df_data['readable_text'].progress_apply(lambda x: clean_text(x))
    if do_lemmatize:
        parser = spacy.load('en', disable=['parser', 'ner'])
        df_data['processed_value'] = df_data['processed_value'].progress_apply(
            lambda x: lemmatize(x, parser))
    if no_stopwords:
        df_data['processed_value'] = df_data['processed_value'].progress_apply(
            lambda x: ' '.join([word for word in x.split() if word not in (text.ENGLISH_STOP_WORDS)]))
    df_data = df_data[pd.notnull(df_data[column_name])]

    return df_data


def preprocessed_nums_in_doc_regex(raw_html):
    try:
        gc.collect()
        raw_html = remove_table_if_no_numeric(raw_html)
        clean_html = re.sub(remove_superscript_tag, '', raw_html)
        #    clean_html = remove_table_head(raw_html)
        numbers_in_table = []
        try:
            tables_list = pd.read_html(clean_html, thousands=None)
            for df in tables_list:
                table_str = df.to_string(header=False, index=False, na_rep='', index_names=False)
                result = re.sub(regex, '', table_str)
                tmp = []
                for item in re.findall(detectNumbersInText, result):
                    try:
                        tmp.append(float(re.sub('\,', '', item)))
                    except ValueError:
                        loggerObj.exception(
                            "Exception occurred in cleaner.preprocessed_nums_in_doc_regex:\tDuring extraction of tables")
                        continue
                numbers_in_table.append(tmp)
        #                numbers_in_table.append([float(re.sub('\,', '', item)) for item in re.findall(detectNumbersInText, result)])
        except ValueError:
            pass
        numbers_in_table = [item for sublist in numbers_in_table for item in sublist]
        numbersTypeTable = [1 for num in numbers_in_table]

        textWithoutTable = remove_all_tables(raw_html)
        textWithoutTable = re.sub(r'>\s*[\s\"\'-\/:;=>\w%]*\">', '>', textWithoutTable)
        textWithoutTable = re.sub(remove_superscript_tag, '', textWithoutTable)
        cleanText = get_readable_text(textWithoutTable)
        #        cleanText = None
        if cleanText is None:
            soup = BeautifulSoup(textWithoutTable, 'lxml')
            cleanText = soup.text
        cleanText = re.sub(removeLeftoverTags, '', cleanText)
        resultText = re.sub(regex, '', cleanText)
        resultText = re.sub(r"\(\d\)", '', resultText)
        resultText = re.sub(detectTitleNumber, '', resultText, count=1)
        numbers_in_text = []
        for item in re.findall(detectNumbersInText, resultText):
            try:
                numbers_in_text.append(float(re.sub('\,', '', item)))
            except ValueError:
                # loggerObj.exception(
                #     "Exception occurred in cleaner.preprocessed_nums_in_doc_regex:\tDuring extraction of text")
                continue
        #        numbers_in_text = [float(re.sub('\,', '', item)) for item in re.findall(detectNumbersInText, resultText)]
        numbersTypeText = [0 for num in numbers_in_text]
        numbers_in_doc = numbers_in_table + numbers_in_text
        numbersType = numbersTypeTable + numbersTypeText
        combined = [str(numbers_in_doc[i]) + str(numbersType[i]) for i in range(len(numbers_in_doc))]
    except:  # return nan?
        # loggerObj.exception("Exception occurred in cleaner.preprocessed_nums_in_doc_regex: Discarding the datapoint")
        combined = []
    if len(combined) == 0:
        return ['02']
    return combined


def format_numbers(sentence):
    sentence = remove_punctuation(sentence)
    sentence = sentence.lower()

    sentence = sentence.replace('.', '')
    numbers = (re.findall('\d+', sentence))

    numbers_dict1 = {}
    numbers_dict2 = {}
    text_initial_version1 = []
    text_initial_version2 = []
    for i in sentence.split():

        if i in numbers:

            next_word1 = 'ENUM' + i

        else:
            next_word1 = i

        text_initial_version1.append(next_word1)

    text_initial_version1 = ' '.join(text_initial_version1)

    # sentence = remove_currency_related_text(sentence)
    new_sentence = remove_specific_numbers(text_initial_version1, 'after')
    #     new_sentence = numbers_final_cleaning(new_sentence)

    for i in new_sentence.split():
        if i in numbers:
            next_word2 = i + 'ENUM'

        else:
            next_word2 = i

        text_initial_version2.append(next_word2)
    #         print(next_word2)
    text_initial_version2 = ' '.join(text_initial_version2)

    new_sentence = remove_specific_numbers(new_sentence, 'before')
    #     print(new_sentence)

    new_sentence = numbers_final_cleaning(new_sentence)

    #     new_sentence = new_sentence.replace('ENUM','')

    return new_sentence


def numbers_final_cleaning(object):
    output_sentence = []
    for i in object.split():

        if i.startswith('ENUM') or i.endswith('ENUM'):

            word = i.replace('ENUM', '')
            if (word.startswith('20') or word.startswith('19')):
                if len(word) != 4:

                    output_sentence.append(word)
            else:
                output_sentence.append(word)
        elif i.isalpha():
            word = i
            output_sentence.append(word)

    #     sentence1 = [t for t in object.split() if t.endswith('ENUM')]
    #     sentence2 = [t for t in sentence1 if t.startswith('ENUM')]

    return ' '.join(output_sentence)


def remove_specific_numbers(old_sentence, place):
    if place == 'after':
        months_dictionary = {'january': 'jan', 'february': 'feb', 'march': 'mar', 'april': 'apr', 'may': 'may',
                             'june': 'july',
                             'august': 'aug', 'september': 'sept', 'october': 'oct', 'november': 'nov',
                             'december': 'dec'
                             }

        old_sentence = replace_numbers_after_text(old_sentence, 'asc')
        old_sentence = replace_numbers_after_text(old_sentence, 'u.s. patent no.')
        old_sentence = replace_numbers_after_text(old_sentence, 'level')
        old_sentence = replace_numbers_after_text(old_sentence, 'topic')
        old_sentence = replace_numbers_after_text(old_sentence, 'chapter')
        #         old_sentence = replace_numbers_after_text(old_sentence, '(')
        # old_sentence = replace_numbers_after_text(old_sentence, '-')
        old_sentence = replace_numbers_after_text(old_sentence, 'chapter')

        for i in months_dictionary:
            old_sentence = replace_numbers_after_text(old_sentence, i)
            old_sentence = replace_text_after_numbers(old_sentence, i)
            old_sentence = replace_numbers_after_text(old_sentence, months_dictionary[i])
            old_sentence = replace_text_after_numbers(old_sentence, months_dictionary[i])
        old_sentence = old_sentence.replace('ENUM.', '')




    elif place == 'before':
        old_sentence = replace_text_after_numbers(old_sentence, 'days')
        old_sentence = replace_text_after_numbers(old_sentence, 'years')
        old_sentence = replace_text_after_numbers(old_sentence, 'months')
        old_sentence = replace_text_after_numbers(old_sentence, 'quarters')
        old_sentence = replace_text_after_numbers(old_sentence, '(k)')
        old_sentence = replace_text_after_numbers(old_sentence, '-')
        old_sentence = replace_text_after_numbers(old_sentence, '$')
    #         old_sentence = old_sentence.replace('ENUM', '')


    return old_sentence


def replace_text_after_numbers(line, keyword):
    line = line.replace('ENUM ' + keyword, ' ' + keyword)
    line = line.replace('ENUM' + keyword, keyword)

    return line


def replace_numbers_after_text(line, keyword):
    line = line.replace(keyword + 'ENUM', keyword)
    line = line.replace(keyword + ' ENUM', keyword + ' ')
    return line

    # from string import digits
    # remove_digits = str.maketrans('', '', digits)
    # list = [i.translate(remove_digits) for i in sentence.split()]
    # return " ".join(list)


def remove_numbers(sentence):
    from string import digits

    remove_digits = str.maketrans('', '', digits)
    list = [i.translate(remove_digits) for i in sentence.split()]
    return " ".join(list)


def remove_punctuation(sentence):
    return sentence.translate(str.maketrans('', '', string.punctuation))


def get_numbers_nontabular_data(row):

    row = format_numbers(row)
    return len(re.findall('\d+', row))
