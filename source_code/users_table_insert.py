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

# total order numbers: 25,000
total_order_nums = cur.execute("select count(order_id) from orders_table")
total_order_nums = cur.fetchone()
total_order_nums = total_order_nums[0]

# total day nums: 60
total_day_nums = 60

# total distinct location numbers: 5
total_locations = cur.execute("select count(distinct(location_id)) from orders_table")
total_locations = cur.fetchone()
total_locations = total_locations[0]

# total price: 
total_price = cur.execute("select sum(CAST(total_price  AS DOUBLE PRECISION)) as price from orders_table")
total_price = cur.fetchone()
total_price = total_price[0]

# total discount:
total_discount = cur.execute("select sum(CAST(total_discounts  AS DOUBLE PRECISION)) as price from orders_table")
total_discount = cur.fetchone()
total_discount = total_discount[0]

# total_tax:
total_tax = cur.execute("select sum(CAST(total_tax  AS DOUBLE PRECISION)) as price from orders_table")
total_tax = cur.fetchone()
total_tax = total_tax[0]

# total product numbers
total_product_nums = cur.execute("select sum(CAST(line_items_quantity AS INTEGER)) from products_table")
total_product_nums = cur.fetchone()
total_product_nums = total_product_nums[0]

# total distinct product numbers: 42
total_dis_product_nums = cur.execute("select count(distinct(line_items_product_id)) from products_table")
total_dis_product_nums = cur.fetchone()
total_dis_product_nums = total_dis_product_nums[0]

# The most populer product: The most quantity it was ordered
most_populer_product = cur.execute("select line_items_product_id, sum(CAST(line_items_quantity as INTEGER)) as con from products_table group by line_items_product_id order by con DESC limit 1")
most_populer_product = cur.fetchone()
most_populer_product = most_populer_product[0]

# The most populer locations: '371291', The most total price, or the most products numbers, or the most order numbers in that location
most_populer_location = cur.execute("select location_id, sum(CAST(line_items_quantity as INTEGER)) as con from products_table group by location_id order by con DESC limit 1")
most_populer_location = cur.fetchone()
most_populer_location = most_populer_location[0]

# The most populer order day: 2017-11-11, The most total price, or the most products numbers, or the most order numbers on that day
most_populer_order_day = cur.execute("select date3, count(order_id) as order_con from orders_table group by date3 order by order_con DESC limit 1")
most_populer_order_day = cur.fetchone()
most_populer_order_day = most_populer_order_day[0]

# The average price per location per day 
average_price_loc_day = total_price / total_locations / total_day_nums

# The price at populer location and pupuler day
price_populer_loc_day = cur.execute("select sum(CAST(total_price  AS DOUBLE PRECISION)) as price from orders_table where location_id = '371291' and date3 = '2017-11-11'")
price_populer_loc_day = cur.fetchone()
price_populer_loc_day = price_populer_loc_day[0]

# The average product numbers per location per day
average_products_loc_day = total_product_nums / total_locations/total_day_nums

# The product numbers at populer location and populer day  
products_populer_loc_day = cur.execute("select sum(CAST(p.line_items_quantity  AS INTEGER)) as con from products_table as p join orders_table as o on o.order_id = p.order_id where p.location_id = '371291' and o.date3 = '2017-11-11'")
products_populer_loc_day = cur.fetchone()
products_populer_loc_day = products_populer_loc_day[0]
       
usertuple = ()
userdict = dict()
userdict['total_order_nums'] = total_order_nums
userdict['total_day_nums']   = total_day_nums
userdict['total_locations']  = total_locations
userdict['total_price']      = total_price
userdict['total_discount']   = total_discount
userdict['total_tax']        = total_tax
userdict['total_product_nums']     = total_product_nums
userdict['total_dis_product_nums'] = total_dis_product_nums
userdict['most_populer_product']   = most_populer_product
userdict['most_populer_location']  = most_populer_location
userdict['most_populer_order_day'] = most_populer_order_day
userdict['average_price_loc_day']  = average_price_loc_day
userdict['price_populer_loc_day']  = price_populer_loc_day
userdict['average_products_loc_day'] = average_products_loc_day
userdict['products_populer_loc_day'] = products_populer_loc_day
usertuple = usertuple + (userdict,)

# insert to postgresql database
try:
    cur.executemany("""INSERT INTO users_table (total_order_nums, total_day_nums, total_locations, total_price, total_discount, total_tax, total_product_nums, total_dis_product_nums, most_populer_product, most_populer_location, most_populer_order_day, average_price_loc_day, price_populer_loc_day, average_products_loc_day, products_populer_loc_day ) \
                   VALUES(%(total_order_nums)s, %(total_day_nums)s, %(total_locations)s, %(total_price)s, %(total_discount)s, %(total_tax)s, %(total_product_nums)s, %(total_dis_product_nums)s, %(most_populer_product)s, %(most_populer_location)s, %(most_populer_order_day)s, %(average_price_loc_day)s, %(price_populer_loc_day)s, %(average_products_loc_day)s, %(products_populer_loc_day)s )""", usertuple)

except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    print("can not insert to postgres")
connection.commit()

cur.close()
connection.close()

