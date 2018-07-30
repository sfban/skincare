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

    ordertuple = ()
    # how many orders in each files
    m = len(data['orders'])
    for i in range(m):
        # for one order, how many dictionary keys: 51
        leng = len(data['orders'][i])
        each_order = data['orders'][i]
        # save the dictionary values for each_order
        orderdict = dict()
        orderdict['order_id'] = each_order['id']
        orderdict['created_at'] = each_order['created_at']
        orderdict['updated_at'] = each_order['updated_at']
        orderdict['processed_at'] = each_order['processed_at']
        orderdict['date1'] = each_order['created_at'][0:10]
        orderdict['date2'] = each_order['updated_at'][0:10]
        orderdict['date3'] = each_order['processed_at'][0:10]
        orderdict['year']  = each_order['processed_at'][0:4]
        orderdict['month'] = each_order['processed_at'][5:7]
        orderdict['day']   = each_order['processed_at'][8:10]
        orderdict['number'] = each_order['number']
        orderdict['order_number'] = each_order['order_number']
        orderdict['user_id'] = each_order['user_id']
        orderdict['location_id'] = each_order['location_id']
        orderdict['source_identifier'] = each_order['source_identifier']
        orderdict['device_id'] = each_order['device_id']
        orderdict['checkout_id'] = each_order['checkout_id']
        orderdict['name'] = each_order['name']
        orderdict['total_price'] = each_order['total_price']
        orderdict['total_price_usd'] = each_order['total_price_usd']
        orderdict['subtotal_price'] = each_order['subtotal_price']
        orderdict['total_weight'] = each_order['total_weight']
        orderdict['total_tax'] = each_order['total_tax']
        orderdict['total_discounts'] = each_order['total_discounts']
        orderdict['total_line_items_price'] = each_order['total_line_items_price']
        orderdict['total_line_items_nums'] = len(each_order['line_items'])
        orderdict['buyer_accepts_marketing'] = each_order['buyer_accepts_marketing']
        orderdict['gateway'] = each_order['gateway']
        orderdict['processing_method'] = each_order['processing_method']
        orderdict['fulfillment_status'] = each_order['fulfillment_status']
        orderdict['reference'] = each_order['reference']
        orderdict['note'] = each_order['note']
        orderdict['test'] = each_order['test']

        ordertuple = ordertuple + (orderdict,)

    # insert to postgresql database
    print(123)
    try:
        cur.executemany("""INSERT INTO orders_table (order_id, created_at, updated_at, processed_at, date1, date2, date3, year, month, day, number, order_number, user_id, location_id, source_identifier, device_id, checkout_id, name, total_price, total_price_usd, subtotal_price, total_weight, total_tax, total_discounts, total_line_items_price, total_line_items_nums, buyer_accepts_marketing, gateway, processing_method, fulfillment_status, reference, note, test) \
                           VALUES(%(order_id)s, %(created_at)s, %(updated_at)s, %(processed_at)s, %(date1)s, %(date2)s, %(date3)s, %(year)s, %(month)s, %(day)s, %(number)s, %(order_number)s, %(user_id)s, %(location_id)s, %(source_identifier)s, %(device_id)s, %(checkout_id)s, %(name)s, %(total_price)s, %(total_price_usd)s, %(subtotal_price)s, %(total_weight)s, %(total_tax)s, %(total_discounts)s, %(total_line_items_price)s, %(total_line_items_nums)s, %(buyer_accepts_marketing)s, %(gateway)s, %(processing_method)s, %(fulfillment_status)s, %(reference)s, %(note)s, %(test)s )""", ordertuple)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        print("can not insert to postgres", k, m)
    connection.commit()

cur.close()
connection.close()
