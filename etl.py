import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Loads both staging tables by executing the copy commands in sql_queries.py"""
    for query in copy_table_queries:
        print("Currently executing", query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Inserts data from the staging tables into the final analytics tables using the corresponding
    queries from sql_queries.py"""
    for query in insert_table_queries:
        print("Currently executing", query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get("CLUSTER","HOST"), config.get("CLUSTER","DB_NAME"),
           config.get("CLUSTER","DB_USER"), config.get("CLUSTER","DB_PASSWORD"),
           config.get("CLUSTER","DB_PORT")))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()