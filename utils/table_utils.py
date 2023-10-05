from bs4 import BeautifulSoup
from utils.preprocessor import remove_numbers, format_numbers, remove_punctuation
import string,re
import pandas as pd

def get_text_from_table(tables):
    trs = tables.findAll('tr')

    texts = []
    for tr in trs:

        tds = tr.findAll('td')

        for td in tds:
            texts.append(td.text)
    # texts = format_numbers(' '.join(texts))
    texts = (' '.join(texts))
    return texts


def get_word_count_from_table(soup):
    texts = get_text_from_table(soup)

    texts = remove_numbers(texts)
    texts = remove_punctuation(texts)
    texts = format_numbers(texts)

    return len(texts.split())


def get_number_count(text):
    # text = format_numbers(text)
    text = remove_punctuation(text)

    return sum(c.isdigit() for c in text)


def is_alphabetical_cell(numbers_count, cell_content):
    cell_content_length = len(cell_content)
    cell_content = cell_content.split(' ')

    punctuation_content = sum(k in string.punctuation for k in cell_content)

    if punctuation_content == len(cell_content):
        return False
    elif cell_content_length == 0:
        return False
    elif numbers_count / cell_content_length <= 0.2:
        return True
    else:
        return False


def is_table_alphabetic_using_cell_rule(all_tables):
    CELL_RULE_PERCENTAGE = 0.4
    word_count = 0
    cou = 0
    for i in all_tables:
        trs = i.findAll('tr')
        alphabetic_cells = 0
        cou = cou + 1

        cell_count = 0
        for tr in trs:
            tds = tr.findAll('td')

            for td in tds:
                if not (td.text).isspace():
                    numbers_count = get_number_count(td.text)
                    is_cell_alphabetic = is_alphabetical_cell(numbers_count, (td.text))
                    cell_count = cell_count + 1

                    if is_cell_alphabetic:
                        alphabetic_cells += 1

        if cell_count > 0:
            if alphabetic_cells / cell_count >= CELL_RULE_PERCENTAGE:
                word_count = word_count + get_word_count_from_table(i)


    return word_count


def get_word_count_from_preprocessed_data(row, training_column):
    word_count = len(row['processed_value'].split())

    if word_count == 0:
        soup = BeautifulSoup(row[training_column], features='lxml')
        table = soup.findAll('table')
        if table:
            word_count = get_word_count_from_table(soup)

    return word_count


def is_table_alphabetic_using_table_rule(all_tables, number_percent_threshold, rule):
    word_count = 0
    count = 0

    for i in all_tables:

        texts = get_text_from_table(i)
        numbers_in_table = get_number_count(texts)
        content = (i.text.replace('\xa0', '')).replace(' ','')

        total_count = len(content)
        count = count + 1

        if total_count > 0:
            result = numbers_in_table / total_count < number_percent_threshold

            if result:

                word_count = word_count + get_word_count_from_table(i)


    return word_count


def get_word_count_using_rules(row, training_column, rule, number_percent_threshold):
    soup = BeautifulSoup(row[training_column], features='lxml')
    table = soup.findAll('table')

    word_count = 0
    if table:

        if rule == 'WRDS':
            word_count = is_table_alphabetic_using_cell_rule(table)

        if rule == 'L&M_jar':
            word_count = is_table_alphabetic_using_table_rule(table, number_percent_threshold, rule)
        elif rule == 'Bodnaruk':
            word_count = is_table_alphabetic_using_table_rule(table, number_percent_threshold, rule)
    return word_count + get_word_count_from_preprocessed_data(row, training_column)


def is_table_all_alphabetic(soup):
    table = soup.findAll('table')
    trs = table[0].findAll('tr')

    cell_count = 0
    alphabetic_cells = 0
    for tr in trs:
        tds = tr.findAll('td')
        for td in tds:
            cell_count += 1
            numbers_count_in_cell = get_number_count(td.text)

            if numbers_count_in_cell > 0:
                return False
            else:
                continue
    return True


def get_word_count(row, training_column):
    soup = BeautifulSoup(row[training_column], features='lxml')
    table = soup.findAll('table')
    is_alphabetic_table = False
    if table:
        is_alphabetic_table = is_table_all_alphabetic(soup)

    return get_word_count_from_table(soup) + get_word_count_from_preprocessed_data(
        row, training_column) if is_alphabetic_table else get_word_count_from_preprocessed_data(
        row, training_column)




def count_numbers_in_table(row):
    soup = BeautifulSoup(row, features='lxml')
    table = soup.findAll('table')


    li = []
    number_count = 0
    for i in table:

        trs = i.findAll('tr')
        for tr in trs:
            tds = tr.findAll('td')

            for td in tds:
                if not (td.text).isspace():
                    tabular_data = format_numbers(td.text)

                    tabular_data = tabular_data.replace(',', '')
                    number_count = number_count + len(re.findall('\d+', tabular_data))

    return number_count


def number_statictics(df,column,primary_key,condition):
    df['entire_html_data'] = df[column].apply(lambda x: BeautifulSoup(x,"lxml").get_text(" ",strip=True))
    df['entire_html_data'] = df['entire_html_data'].apply(lambda x:x.replace('\xa0', ' '))
    df['entire_html_data'] = df['entire_html_data'].apply(lambda x: x.replace(',', ''))
    df['entire_html_data'] = df['entire_html_data'].apply(lambda x: format_numbers(x))

    df['total_numbers'] = df['entire_html_data'].apply(lambda x: (re.findall('\d+', x)))

    if condition:
        primary_key_numbers_dict = dict(zip(df[primary_key],df['total_numbers']))
        numders_df = pd.DataFrame()
        for i in primary_key_numbers_dict:
            dummy_df = pd.DataFrame(primary_key_numbers_dict[i],columns=['numbers_in_data'])
            dummy_df[primary_key] = i
            dummy_df = dummy_df[[primary_key,'numbers_in_data']]
            numders_df = pd.concat([numders_df,dummy_df])
        df['total_numbers'] = df['entire_html_data'].apply(lambda x: len(re.findall('\d+', x)))
        df = df.drop('entire_html_data',axis = 1)
        return df,numders_df
    else:
        df['total_numbers'] = df['entire_html_data'].apply(lambda x: len(re.findall('\d+', x)))
        df = df.drop('entire_html_data', axis=1)
        return df