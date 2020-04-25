import psycopg2
import configparser

def get_table_counts(cur):
    """ Gets the table counts for all the tables. These are to be compared 
    with https://knowledge.udacity.com/questions/57277"""
    base_query = 'SELECT COUNT(*) FROM'
    tables = ['staging_events', 'staging_songs', 'songplays', 'users',
             'songs', 'artists', 'time']
    with open('table_frequencies.txt', 'w') as outfile:
        for table in tables:
            cur.execute("{} {}".format(base_query, table))
            num_rows = cur.fetchone()[0]
            outfile.write("No. of rows in {} is {}.\n".format(table, num_rows))
            print("No. of rows in {} is {}.".format(table, num_rows))
    
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get("CLUSTER","HOST"), config.get("CLUSTER","DB_NAME"),
           config.get("CLUSTER","DB_USER"), config.get("CLUSTER","DB_PASSWORD"),
           config.get("CLUSTER","DB_PORT")))
    cur = conn.cursor()
    get_table_counts(cur)
    conn.close()


if __name__ == "__main__":
    main()