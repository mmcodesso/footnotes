import re
import sys
import pandas as pd

sys.path.append("..")
from tqdm import tqdm

from utils.connector import dataframe_to_postgres, table_exists, insert_many, delete_table
from dataloader.get_data import get_table_with_limit, get_rows, get_table_column
from utils.table_utils import get_word_count, get_word_count_from_preprocessed_data, get_word_count_using_rules, \
    number_statictics
from utils.preprocessor import format_numbers, preprocess_column, get_numbers_nontabular_data
import logging

from configs import config


def get_word_count_for_df(df_data, training_column, numbers_condition, primary_key, input_data_condition):
    tqdm.pandas()

    df_data = preprocess_column(df_data, training_column, False, False)

    df_data['processed_value'] = df_data["processed_value"].progress_apply(lambda row: format_numbers(row))

    logging.info("Deriving different word count statistics...")
    df_data['word_count_only_text'] = df_data.progress_apply(
        lambda row: get_word_count_from_preprocessed_data(row, training_column), axis=1)
    df_data['word_count_with_table_rule'] = df_data.progress_apply(lambda row: get_word_count(row, training_column),
                                                                   axis=1)

    logging.info("Applying cell rule...")

    df_data['word_count_WRDS_rule'] = df_data.progress_apply(
        lambda row: get_word_count_using_rules(row, training_column, 'WRDS', 0), axis=1)
    logging.info("Applying table rule...")

    df_data['word_count_L&M_rule'] = df_data.progress_apply(
        lambda row: get_word_count_using_rules(row, training_column, 'L&M_jar', 0.1), axis=1)

    df_data['word_count_Bodnaruk_rule'] = df_data.progress_apply(
        lambda row: get_word_count_using_rules(row, training_column, 'Bodnaruk', 0.15), axis=1)

    df_data['numbers_in_non_tabular_data'] = df_data['processed_value'].progress_apply(
        lambda x: get_numbers_nontabular_data(x))

    if numbers_condition:
        df_data, df_numbers_data = number_statictics(df_data, training_column, primary_key, numbers_condition)
        logging.info("Word count and number count finished for the batch")
        df_data = remove_input_columns(df_data, input_data_condition, training_column)
        return df_data, df_numbers_data
    else:
        df_data = number_statictics(df_data, training_column, primary_key, numbers_condition)
        logging.info("Word count finished for the batch")
        df_data = remove_input_columns(df_data, input_data_condition, training_column)
        return df_data, pd.DataFrame()


def remove_input_columns(df, condition, input_column):
    if not condition:
        df = df.drop([input_column, 'processed_value', 'readable_text'], axis=1)
    return df


def df_to_records(df):
    return [list(row) for row in df.itertuples(index=False)]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    cfg = config.read()

    training_column = cfg.get('postgres', 'column')
    pk = cfg.get('postgres', 'primary_key')
    input_table_name = cfg.get("postgres", "table")
    output_table_name = cfg.get("postgres", "output_table_name")
    output_numbers_table_name = cfg.get("postgres", "output_numbers_table_name")
    batch_size = cfg.getint('postgres', 'batch_size')
    append = cfg.getboolean('postgres', 'append')
    numbers_table_condition = cfg.getboolean('conditions', 'create_numbers_table')
    include_input_data = cfg.getboolean('conditions', 'include_input_data')

    print("Reading table in batch mode")
    total_batches = get_rows(input_table_name) // batch_size
    totol_row_count = get_rows(input_table_name)

    if not append:

        logging.info("Append is False. Hence output table must not exists.")
        if table_exists(output_table_name):
            logging.info("Deleting the table...")
            delete_table(output_table_name)

    start_index = 0
    while start_index < totol_row_count:

        df_data = get_table_with_limit(input_table_name, pk, batch_size, start_index)

        if append:
            stored_pks = get_table_column(output_table_name, pk)[pk].values
            df_data = df_data[~df_data[pk].isin(stored_pks)]

            logging.info("Calculating word counts form {} to {}".format(start_index, start_index + batch_size))

            if not df_data.empty:
                df_data, df_numbers_data = get_word_count_for_df(df_data, training_column, numbers_table_condition, pk,
                                                                 include_input_data)

                results = df_to_records(df_data)
                insert_many(output_table_name, results)
                if len(df_numbers_data) > 0:
                    numbers_results = df_to_records(df_numbers_data)
                    insert_many(output_numbers_table_name, numbers_results)
            else:
                logging.info("No new points available from {} to {}".format(start_index, start_index + batch_size))

        else:
            df_data, df_numbers_data = get_word_count_for_df(df_data, training_column, numbers_table_condition, pk,
                                                             include_input_data)

            if table_exists(output_table_name):
                logging.info("Calculating word counts from {} to {}".format(start_index, start_index + batch_size))

                results = df_to_records(df_data)

                logging.info("Inserting the records...")
                insert_many(output_table_name, results)
                if len(df_numbers_data) > 0:
                    numbers_results = df_to_records(df_numbers_data)
                    insert_many(output_numbers_table_name, numbers_results)
                logging.info("Records inserted.")
            else:
                logging.info(
                    "Table does not exist. Hence creating new table {} and dumping results from {} to {}".format(
                        output_table_name, start_index, start_index + batch_size))
                dataframe_to_postgres(df_data, output_table_name, "False")
                if len(df_numbers_data) > 0:
                    dataframe_to_postgres(df_numbers_data, output_numbers_table_name, "False")
                logging.info("Table created.")

        start_index += batch_size
    logging.info("Code run successful!")
