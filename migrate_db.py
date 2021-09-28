import os
import sys
import psycopg2
from pprint import pprint
import MySQLdb
import pymysql
pymysql.install_as_MySQLdb()
import mysql.connector as msql
from mysql.connector import Error
# Import internal snippets     
#from include.db_config import *
#from include.MySQLCursorDict import *

# Open database connections   
# Mysql connection
try:
    conn_mysql= msql.connect(host='localhost', database='sensor', user='root', password='password')
    print('Connected to MySQL Database')
except Error as e:
    print("Error while connecting to MySQL", e)
    sys.exit(1)
# Postgresql connection
try:
    conn_psql = psycopg2.connect(host='localhost', database='sensorDWH', user='postgres', password='password')
    print('Connected to PostgreSQL Database')
except psycopg2.Error as e:
    print('PSQL: Unable to connect!\n{0}').format(e)
    sys.exit(1)
    
 # Cursors initializations
cur_mysql = conn_mysql.cursor()
cur_psql = conn_psql.cursor()

 #create table
cur_mysql.execute("SELECT ID, flow_99, flow_max, flow_median, flow_total, n_obs FROM station_summary.load_data3")

create_psql_table = '''CREATE TABLE load_data3 (
                        ID INT, flow_99 FLOAT, flow_max FLOAT, flow_median FLOAT, flow_total FLOAT, n_obs FLOAT)'''
cur_psql.execute(create_psql_table)

for row in cur_mysql:
    try:
        cur_psql.execute("INSERT INTO first_data.load_data3 VALUES (%s,%s,%s,%s,%s,%s)", row)
    except psycopg2.Error as e:
        print("cannot execute that query!!", e.pgerror)
        sys.exit("Some problem occured with that query!")   
## Closing cursors
cur_mysql.close()
cur_psql.close()

## Committing 
conn_psql.commit()

## Closing database connections
conn_mysql.close()
conn_psql.close()