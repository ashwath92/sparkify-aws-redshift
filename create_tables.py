import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops staging and analytics tables based on the queries in sql_queries.py""""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Creates staging and analytics tables based on the queries in sql_queries.py"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Main function"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get("CLUSTER","HOST"), config.get("CLUSTER","DB_NAME"),
           config.get("CLUSTER","DB_USER"), config.get("CLUSTER","DB_PASSWORD"),
           config.get("CLUSTER","DB_PORT")))
    cur = conn.cursor()
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()