import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

#Drop any existing tables from sparkifydb
def drop_tables(cur, conn):
    """ args:
    * cur  --   cursory to connected DB. Allows to execute SQL commands.
    * conn --   (psycopg2) connection to Postgres database (sparkifydb).
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue dropping table: " + query)
            print(e)

    print("Tables dropped.")

#Create new tables in sparkifydb
def create_tables(cur, conn):
    """ args:
    * cur  --   cursory to connected DB. Allows to execute SQL commands.
    * conn --   (psycopg2) connection to Postgres database (sparkifydb).
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue creating table: " + query)
            print(e)
            
    print("Tables created.")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()