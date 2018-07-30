import json
import pandas
from pprint import pprint
import psycopg2
from config_pg import *

def create_tables():
    """create the first order tables in PostgresSQL database"""
    commands = (
        """ 
        CREATE TABLE orders_table (
            order_id VARCHAR(255) PRIMARY KEY,
            created_at VARCHAR(255) NOT NULL,
            updated_at VARCHAR(255) NOT NULL,
            processed_at VARCHAR(255) NOT NULL,
            date1 VARCHAR(255) NOT NULL,
            date2 VARCHAR(255) NOT NULL,
            date3 VARCHAR(255) NOT NULL,
            year VARCHAR(255) NOT NULL,
            month VARCHAR(255) NOT NULL,
            day VARCHAR(255) NOT NULL,
            number VARCHAR(255) NOT NULL,
            order_number VARCHAR(255) NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            location_id VARCHAR(255) NOT NULL,
            source_identifier VARCHAR(255) NOT NULL,
            device_id VARCHAR(255) NOT NULL,
            checkout_id VARCHAR(255),
            name VARCHAR(255) NOT NULL,
            total_price VARCHAR(255) NOT NULL,
            total_price_usd VARCHAR(255) NOT NULL,
            subtotal_price VARCHAR(255) NOT NULL,
            total_tax VARCHAR(255) NOT NULL,
            total_discounts VARCHAR(255) NOT NULL,
            total_weight VARCHAR(255) NOT NULL,
            total_line_items_price VARCHAR(255) NOT NULL,
            total_line_items_nums VARCHAR(255) NOT NULL,
            buyer_accepts_marketing VARCHAR(255) NOT NULL,
            gateway VARCHAR(255) NOT NULL,
            processing_method VARCHAR(255),
            fulfillment_status VARCHAR(255),
            reference VARCHAR(255),
            note VARCHAR(255),
            test VARCHAR(255)
        )
        """
        ,
        """
        CREATE TABLE products_table (
            serial_id SERIAL PRIMARY KEY,
            order_id VARCHAR(255) NOT NULL,
            location_id VARCHAR(255) NOT NULL,
            total_line_items_price VARCHAR(255) NOT NULL,
            total_line_items_nums VARCHAR(255) NOT NULL,
            line_items_id VARCHAR(255) NOT NULL,
            line_items_variant_id VARCHAR(255),
            line_items_quantity VARCHAR(255) NOT NULL,
            line_items_product_id VARCHAR(255)
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
