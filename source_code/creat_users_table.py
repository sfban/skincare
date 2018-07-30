import json
import pandas
from pprint import pprint
import psycopg2
from config_pg import *

def create_tables():
    """create the first order tables in PostgresSQL database"""
    commands = (
        """ 
        DROP TABLE users_table
        """
        ,
        """
        CREATE TABLE users_table (
            serial_id SERIAL PRIMARY KEY,
            total_order_nums VARCHAR(255) NOT NULL,
            total_day_nums VARCHAR(255) NOT NULL,
            total_locations VARCHAR(255) NOT NULL,
            total_price VARCHAR(255) NOT NULL,
            total_discount VARCHAR(255) NOT NULL,
            total_tax VARCHAR(255) NOT NULL,
            total_product_nums VARCHAR(255) NOT NULL,
            total_dis_product_nums VARCHAR(255) NOT NULL,
            most_populer_product VARCHAR(255) NOT NULL,
            most_populer_location VARCHAR(255) NOT NULL,
            most_populer_order_day VARCHAR(255) NOT NULL,
            average_price_loc_day VARCHAR(255) NOT NULL,
            price_populer_loc_day VARCHAR(255) NOT NULL,
            average_products_loc_day VARCHAR(255) NOT NULL,
            products_populer_loc_day VARCHAR(255) NOT NULL
        )
        """
    ) 
    conn = None
    #connect to the AWS RDS postgresql 
    try:
        conn = psycopg2.connect("dbname='{0}'".format(dbname) +
                              "user='{0}'".format(dbuser) +
                              "host='{0}'".format(dbhost) +
                              "password='{0}'".format(dbpassword))
        
        cur = conn.cursor()
        for command in commands:
           cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        print("I am unable to connect to the database.")
    if conn is not None:
        conn.close()

if __name__ == '__main__':
    create_tables()
