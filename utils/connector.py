import os
import sys

sys.path.append("..")
from configs import config
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

cfg = config.read()
host = cfg.get("postgres", "host")
database = cfg.get("postgres", "database")
user = cfg.get("postgres", "user")
password = cfg.get("postgres", "password")
# table = cfg.get("comment_crawler", "input_table_name")
# pk = cfg.get('comment_crawler', 'primary_key')
port = 5432


def _getpostgres_connection():
    """
    """
    conn_str = "host={} dbname={} user={} password={}".format(host, database, user, password)
    conn = psycopg2.connect(conn_str)

    return conn





def postgres_to_dataframe(table):
    """

    """
    conn = _getpostgres_connection()
    data = pd.read_sql('select * from ' + table, con=conn)
    conn.close()
    return data








def dataframe_to_postgres(df, tablename, db_append):
    connection_string = "postgresql+psycopg2://" + user + ":" + password + "@" + \
                        host + ":" + str(port) + "/" + database
    engine = create_engine(connection_string)
    if db_append.lower() == 'true':
        df.to_sql(tablename, con=engine, if_exists='append', index=False)
    else:
        df.to_sql(tablename, con=engine, if_exists='replace', index=False)
    engine.dispose()

def postgres_to_dataframe_with_limits(table, pk,  limit='ALL', offset=0):
    conn = _getpostgres_connection()

    sql_string = 'select * from ' + table + ' order by ' + '"'+pk+'"'+' limit ' + str(
        limit) + ' offset ' + str(offset)

    data = pd.read_sql(sql_string, con=conn)

    conn.close()
    return data

def get_total_rows(tablename):
    conn = _getpostgres_connection()
    cur = conn.cursor()
    select = "select count(*) from " + tablename
    cur.execute(select)
    row_count = cur.fetchone()[0]
    conn.commit()
    conn.close()
    return row_count

def get_postgres_column(column, table):
    conn = _getpostgres_connection()
    data = pd.read_sql('select ' + column + ' from ' + table, con=conn)
    conn.close()
    return data

def delete_table(tablename):
    conn = _getpostgres_connection()
    cur = conn.cursor()

    delete = """Drop table if exists """ + tablename
    cur.execute(delete)
    conn.commit()
    conn.close()

def insert_many(tablename, items):
    conn = _getpostgres_connection()
    cur = conn.cursor()

    for item in items:
        cols = "%s," * len(item)
        cols = "(" + cols
        cols = cols[:len(cols) - 1] + ")"
        q =  "Insert into " + tablename + " values" + cols

        cur.execute(q,item)

    conn.commit()
    conn.close()

def table_exists(tablename):
    dbcon = _getpostgres_connection()
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True

    dbcur.close()
    return False





