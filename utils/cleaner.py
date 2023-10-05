import os
import re
import signal
import string
import subprocess

import nltk
from bs4 import BeautifulSoup
from nltk.stem.snowball import SnowballStemmer

# from readcalc import readcalc
# from textstat import textstat

__author__ = "Sreejith Sreekumar", "Prasanth Murali"
__email__ = "sreekumar.s@husky.neu.edu", "murali.pr@husky.neu.edu"
__version__ = "0.0.2"

null = 0
empty = 0

foo = []


def lemmatize(passage):
    lemma = nltk.wordnet.WordNetLemmatizer()
    return " ".join(str(x) for x in ([lemma.lemmatize(word) for word in passage.split(" ")]))


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

    import ipdb
    ipdb.set_trace()

    return output


def get_readable_text(raw_html):
    """
    Arguments:
    - `x`:
    """
    '''
    raw_html = bytes(raw_html, 'utf-16').decode("utf-16", 'ignore')
    _cleantext = BeautifulSoup(raw_html, 'lxml').text
    '''
    raw_html = bytes(raw_html, 'utf-16').decode("utf-16", 'ignore')
    _cleantext = BeautifulSoup(raw_html, 'lxml')
    for e in _cleantext.findAll('br'):
        e.replace_with(" ")
    _cleantext = _cleantext.getText(separator=u' ')

    #    paragraphs = _cleantext.split("\n+")
    paragraphs = [s.strip() for s in _cleantext.splitlines()]
    cleaned_paragraphs = []
    for para in paragraphs:
        cleantext = " ".join(para.split())
        cleantext = ''.join(x for x in cleantext if x in string.printable)
        cleaned_paragraphs.append(cleantext)

    cleantext = "\n".join(cleaned_paragraphs)
    strs = re.sub('\\n+', '. ', cleantext)
    cleantext = re.sub(r'\.+', ".", strs)
    return cleantext


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


def extract_fog_score(cleantext):
    calc = readcalc.ReadCalc(cleantext)
    fog_index = calc.get_gunning_fog_index()

    # fog_index2 = textstat.textstat.gunning_fog(cleantext)

    # https://github.com/mmautner/readability
    # readability = Readability(cleantext)
    # fog_index3 = Readability.GunningFogIndex()

    # import ipdb
    # ipdb.set_trace()
    return fog_index
