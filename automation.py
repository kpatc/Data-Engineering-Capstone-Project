# Import libraries required for connecting to mysql
import mysql.connector
# Import libraries required for connecting to DB2 or PostgreSql
import psycopg2
# Connect to MySQL
connection = mysql.connector.connect(user='root', password='ODAwOS1yYW1lc2hz',host='127.0.0.1',database='sales')
# create cursor
cursor_my = connection.cursor()
# connectction details
dsn_hostname = '127.0.0.1'
dsn_user='postgres'      
dsn_pwd ='NjM4OS1qb3N1ZWtw'
dsn_port ="5432"                
dsn_database ="postgres"
# Connect to  PostgreSql
conn = psycopg2.connect(
   database=dsn_database, 
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port= dsn_port
)
#Create a cursor object using cursor() method

cursor_pg = conn.cursor()

# Find out the last rowid from PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.

def get_last_rowid():
    cursor_pg.execute('SELECT * from products;')
    last_id = cursor_pg.fetchall()
    conn.commit()
    return last_id
	


last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    cursor_my.execute("SELECT * FROM sales WHERE rowid>rowid_g")
    rec=cursor_my.fetchall()
    return rec
		

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in  PostgreSql.

def insert_records(records):
	for row in records:
		SQL='INSERT INTO sales_data(rowid,product_id,customer_id,price,quantity,timeestamp) values(%s,%s,%s,%s,%s,%s)' 
		cursor_pg.execute(SQL,row)
		conn.commit()
	

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
connectction.close()
# disconnect from PostgreSql data warehouse 
  conn.close()
# End of program
