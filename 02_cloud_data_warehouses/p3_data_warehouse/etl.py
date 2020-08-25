import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load data from json files in S3 to staging tables on Redshift using queries in sql_queries.py."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Transform data from staging tables and insert into analytic tables on Redshift using queries in sql_queries.py."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Extract song data and log event data from S3, load and transform this data via staging tables on Redshift and  finally load it into analytics tables."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()