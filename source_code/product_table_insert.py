import json
import pandas
from pprint import pprint
import psycopg2
from config_pg import *

#connect to the AWS RDS postgresql 
try:
    connection = psycopg2.connect("dbname='{0}'".format(dbname) +
                              "user='{0}'".format(dbuser) +
                              "host='{0}'".format(dbhost) +
                              "password='{0}'".format(dbpassword))
except:
    print("I am unable to connect to the database.")
cur = connection.cursor()


# Read in 60 files, save to tuple results()
path='/Users/shufang/Documents/Insight_fellow/Glossier_oa/data/2017-'
results = ()
for i in range(10,13):
    month = '{0:02}'.format(i)
    for j in range(1, 32):
        if(i == 10 and j < 30): continue
        if(i > 10 and j > 30): continue
        if(i == 11 and j == 23): continue
        if(i == 12 and j == 25): continue
        day = '{0:02}'.format(j)
        filename = path + month + '-' + day + '.json'
        print(filename)
        with open(filename) as json_file:  
            data = json.load(json_file)
            results = results + (data,)

# insert to postgres table
# 60 files, length of tupe results,
n = len(results)
count = [0 for j in range(51)] 
for k in range(n):
    data = results[k]

    # how many orders in each files
    m = len(data['orders'])
    for i in range(m):
        # for one order, how many dictionary keys: 51
        leng = len(data['orders'][i])
        each_order = data['orders'][i]
   
        ordertuple = ()    

        # length of one line_items list
        items_nums = len(each_order['line_items'])
        for j in range(items_nums):
            jth_line_items = each_order['line_items'][j]
            orderdict = dict()
            orderdict['order_id'] = each_order['id']
            orderdict['location_id'] = each_order['location_id']
            orderdict['total_line_items_price'] = each_order['total_line_items_price']
            orderdict['total_line_items_nums'] = len(each_order['line_items'])
            orderdict['line_items_id'] = jth_line_items['id']
            orderdict['line_items_variant_id'] = jth_line_items['variant_id']
            orderdict['line_items_quantity'] = jth_line_items['quantity']
            orderdict['line_items_product_id'] = jth_line_items['product_id']

            ordertuple = ordertuple + (orderdict,)

        # insert to postgresql database
        try:
            cur.executemany("""INSERT INTO products_table (order_id, location_id, total_line_items_price, total_line_items_nums, line_items_id, line_items_variant_id, line_items_quantity, line_items_product_id) \
                           VALUES(%(order_id)s, %(location_id)s, %(total_line_items_price)s, %(total_line_items_nums)s, %(line_items_id)s, %(line_items_variant_id)s, %(line_items_quantity)s, %(line_items_product_id)s )""", ordertuple)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            print("can not insert to postgres", k, m)
        connection.commit()

cur.close()
connection.close()


