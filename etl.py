import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# Load JSON input data from S3 to insert to staging_events and staging_songs tables.
def load_staging_tables(cur, conn):
    """
    arguments:
    * cur  --   reference to connected db.
    * conn --   parameters (host, dbname, user, password, port)
                to connect the DB.
    """
    
    for query in copy_table_queries:
        print('********************')
        print('Processing: {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('{} processed.'.format(query))
        print('********************')
    
    print('All files copied.')

# Insert data from staging tables into star schema analytics tables
def insert_tables(cur, conn):
    """
    arguments:
    * cur  --   reference to connected db.
    * conn --   parameters (host, dbname, user, password, port)
                to connect the DB.
    """
    for query in insert_table_queries:
        print('********************')
        print('Processing: {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('{} processed.'.format(query))
        print('********************')
        
    print('All data from files inserted.')

#Connect to DB and call functions
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("AWS connection established.")
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()