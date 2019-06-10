import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn, dwh_role_arn, region):
# Open for loop and run the copy queries to load data to staging tables.
    for query in copy_table_queries:
        cur.execute(query.format(dwh_role_arn,region ))
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    REGION = 'us-west-2'
    dwh_role_arn = config.get("DWH","dwh_role_arn")

    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get("DWH","DWH_ENDPOINT"), config.get("DWH","DWH_DB"), \
            config.get("DWH","DWH_DB_USER"), config.get("DWH","DWH_DB_PASSWORD"), config.get("DWH","DWH_PORT")))
    cur = conn.cursor()
    
    
    load_staging_tables(cur, conn, dwh_role_arn, REGION)
    insert_tables(cur, conn)

    
    conn.close()


if __name__ == "__main__":
    main()