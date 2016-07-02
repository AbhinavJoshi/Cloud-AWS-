#Name: Abhinav Joshi
#Class: CSE 6331-004
#Lab Assignment 4
# Import the SDK - boto
import sys
import boto
from boto.s3.key import Key
import time
import csv
import MySQLdb
import random
import memcache


_BUCKET_NAME = 'vehicledata'
_CSV_FILE = 'TLC_Vehicle_Insurance.csv'

#Memcached Parameters
_MEMCACHE_END_POINT='vehicledata.wzfrno.cfg.usw2.cache.amazonaws.com:11211'
memc = memcache.Client([_MEMCACHE_END_POINT])

#DB connection parameters
_RDS_HOST_ADDR = 'vehicledata.c53i3bit4kf6.us-west-2.rds.amazonaws.com'
_RDS_PORT = 3306
_DATABASE_NAME = 'vehicledata'
_TABLE_NAME = 'Vehicle_Insurance'


#Upload def to move the file from local to S3 Storage
def upload_file(connection):

    try:
        bucket_exist = connection.lookup(_BUCKET_NAME)
        if bucket_exist is None:
            connection.create_bucket(_BUCKET_NAME)

        # Upload file part
        file_name = raw_input('Enter file name to be uploaded to Amazon S3\n')
        bucket = connection.get_bucket(_BUCKET_NAME)
        file = Key(bucket)
        start = time.time()
        file.key = file_name
        file.set_contents_from_filename(file_name)
        end = time.time()
        duration = end - start
        print('Upload took {} seconds'.format(duration))
        _CSV_FILE = file_name
    except Exception, exe:
        print exe
        raise

# Create a connection to the database object and returns it 
def get_db():
    print 'Establishing db connection to - {}'.format(_DATABASE_NAME)
    db = MySQLdb.connect(host=_RDS_HOST_ADDR, port=_RDS_PORT, db=_DATABASE_NAME, user='Abhinavjoshi', passwd='rootroot')
    return db

# Creates table as per the name given, if not already created     
def create_table(db):
    print 'Entering create_table function'
    cursor = None
    try:
        create_query = 'CREATE TABLE IF NOT EXISTS {}.{}(\
                        TLC_License_Type CHAR (10),\
						TLC_License_Number VARCHAR(10),\
						DMV_Plate VARCHAR(15),\
						VIN VARCHAR(10),\
						Automobile_Insurance_Code INT(5),\
						Automobile_Insurance_Policy_Number VARCHAR(25),\
						Vehicle_Owner_Name VARCHAR(30),\
						Affiliated_Base_or_Taxi_Agent_or_Fleet_License_Number\
						VARCHAR(15))'.format(_DATABASE_NAME, _TABLE_NAME, _TABLE_NAME)
        cursor = db.cursor()
        cursor.execute(create_query)
        db.commit()
    except MySQLdb.Error, msqe:
        raise
    finally:
        if cursor:
            cursor.close()


def populate_db(db):
    print 'Entering populate_db function'
    cursor = csv_file = None
    try:
        insert_query = 'INSERT INTO {}.{}(\
                TLC_License_Type, TLC_License_Number, DMV_Plate, VIN,\
				Automobile_Insurance_Code, Automobile_Insurance_Policy_Number, \
                Vehicle_Owner_Name, Affiliated_Base_or_Taxi_Agent_or_Fleet_License_Number)\
                VALUES (%s, %s, %s, %s, %s, %s, %s, \
				%s)'.format(_DATABASE_NAME, _TABLE_NAME)
        
        csv_file =  open(_CSV_FILE, 'rb')
        reader = csv.DictReader(csv_file)

        # Reads each row in CSV, creates an array of 1000 entries and does batch insert
        counter = 0
        entries_list = []
        cursor = db.cursor()
        for row in reader:
            entries_list.append((row['TLC_License_Type'], row['TLC_License_Number'], row['DMV_Plate'],
                                row['VIN'], row['Automobile_Insurance_Code'], row['Automobile_Insurance_Policy_Number'],
                                row['Vehicle_Owner_Name'], row['Affiliated_Base_or_Taxi_Agent_or_Fleet_License_Number']))
            counter = counter + 1
            # Once every five thoudsand records, insert and commit is done
            if counter%5000 == 0:
                cursor.executemany(insert_query, entries_list)
                db.commit()
                del entries_list[:]

        if counter%5000 != 0:
            cursor.executemany(insert_query, entries_list)
            db.commit()
            del entries_list[:]
        
    except MySQLdb.Error, msqe:
        raise
    except Exception, e:
        raise
    finally:
        if cursor:
            cursor.close()
        if csv_file:
            csv_file.close()

def import_data(connection):
    print 'Entering import_data function'
    bucket = connection.get_bucket(_BUCKET_NAME)
    file = Key(bucket)
    db = None
    start = time.time()
    try:
        file.key = _CSV_FILE
        file.get_contents_to_filename(_CSV_FILE)
        db = get_db()
        create_table(db)
        #truncate_table(db)
        populate_db(db)
        
    except MySQLdb.Error, msqe:
        raise
    except Exception, e:
        raise
    finally:
        if db and db.open:
            db.close()
    end = time.time()
    duration = end - start
    print 'Importing data from S3 to RDS took {} seconds'.format(duration)

    
def get_data_set_size(db):
    size = 0
    cursor = None
    try:
        query = 'SELECT COUNT(1) FROM {}.{}'.format(_DATABASE_NAME, _TABLE_NAME)
        cursor = db.cursor()
        cursor.execute(query)
        size = cursor.rowcount
    except MySQLdb.Error, msqe:
        raise
    except Exception, e:
        raise
    finally:
        if cursor:
            cursor.close()
    return size

def mem_random_queries(connection):
    print 'Entering mem_random_queries function'
    db = None
    try:
        db = get_db()
        data_size = get_data_set_size(db)
        duration = mem_random_query(db, 1000, int(data_size/1000))
        print('Took {} seconds to do 1000 random queries'.format(duration))
        duration = mem_random_query(db, 5000, int(data_size/5000))
        print('Took {} seconds to do 5000 random queries'.format(duration))
        duration = mem_random_query(db, 20000, int(data_size/20000))
        print('Took {} seconds to do 20000 random queries'.format(duration))
        
    except MySQLdb.Error, msqe:
        raise
    except Exception, e:
        raise
    finally:
        if db and db.open:
            db.close()

#Does random querying for 1000, 5000, 20000 samples.
#Randomization acheived using LIMIT and OFFSET in queries
#Parts of code referenced from -
#http://dev.mysql.com/doc/mysql-ha-scalability/en/ha-memcached-interfaces-python.html
#http://blog.echolibre.com/2009/11/memcache-and-python-getting-started/
def mem_random_query(db, query_count, limit):
    # Parameter used to get a random param value
    zipcode = random.randint(1000, 50000)
    print ('Starting random querying using memcached for {} entries, param value - {}'.format(query_count, zipcode))
    cursor = None
    counter = offset = 0
    start = time.time()
    try:
        random_query = 'SELECT COUNT(1) FROM {}.{} WHERE Automobile_Insurance_Code = %s\
						LIMIT %s OFFSET %s'.format(_DATABASE_NAME, _TABLE_NAME)
        cursor = db.cursor()
        while (counter < query_count):
            key = '{}_{}'.format(query_count, offset)
            data = memc.get(key)
            if not data:
                cursor.execute(random_query, (zipcode, limit, offset))
                rows = cursor.fetchall()
                memc.set(key, rows, 60)
            offset = offset + limit
            counter = counter + 1
    except MySQLdb.Error, msqe:
        raise
    except Exception, e:
        raise
    finally:
        if cursor:
            cursor.close()
    end = time.time()
    duration = end - start
    return duration


# Wrapper method for doing predefined queries using Memcached.
# Calls mem_random_query with sample sizes 1000, 5000, 20000.
def mem_predefined_queries(connection):
    print 'Entering mem_predefined_queries function'
    db = None
    Automobile_Insurance_Code = '347' # A value with 642 entries in the data set
    try:
        db = get_db()
        #Run random queries 1000, 5000, 20000 times
        duration = mem_predefined_query(db, 1000, Automobile_Insurance_Code)
        print('Took {} seconds to do 1000 random queries'.format(duration))
        duration = mem_predefined_query(db, 5000, Automobile_Insurance_Code)
        print('Took {} seconds to do 5000 random queries'.format(duration))
        duration = mem_predefined_query(db, 20000, Automobile_Insurance_Code)
        print('Took {} seconds to do 20000 random queries'.format(duration))
        
    except MySQLdb.Error, msqe:
        raise
    except Exception, e:
        raise
    finally:
        if db and db.open:
            db.close()

# Does querying of a predefined query for 1000, 5000, 20000 samples.
# Also uses Memcached. Returns duration it took to complete the method
# Parts of code referenced from -
# http://dev.mysql.com/doc/mysql-ha-scalability/en/ha-memcached-interfaces-python.html
def mem_predefined_query(db, query_count, param_value):
    print ('Starting defined querying using memcached for {} entries'.format(query_count))
    cursor = None
    counter = 0
    start = time.time()
    try:
        defined_query = 'SELECT DMV_Plate,VIN \
                        FROM {}.{} WHERE Automobile_Insurance_Code = %s'.format(_DATABASE_NAME, _TABLE_NAME)
        cursor = db.cursor()
        key = '{}_{}'.format(query_count, param_value.replace (" ", "_"))
        while (counter < query_count):
            # Checks if memcached has an entry by the key given.
            # If not, fetch results from DB and sets in memcached
            data = memc.get(key)
            if not data:
                cursor.execute(defined_query, (param_value))
                rows = cursor.fetchall()
                memc.set(key, rows, 60)
                
            counter = counter + 1
    except MySQLdb.Error, msqe:
        raise
    except Exception, e:
        raise
    finally:
        if cursor:
            cursor.close()
    end = time.time()
    duration = end - start
    return duration
	
    
def main(argv):
    #Set connection object to S3
    connection = boto.connect_s3('AKIAJDA26BP7XE7IY3EA','9k6IzCtN6lfPgBOIC72qvc+0oEnqnn/rBhyXGlfK')
    
    #Store the option and name of the function as the key value pair in the dictionary.
    options = {1: upload_file, 2: import_data, 3: mem_predefined_queries, 4:mem_random_query, 5:mem_random_queries, 6:get_data_set_size}
    while True:
        option = input('Select the operation to be run \n1:Upload data file, \n2:Import data to RDS, \n3:mem_predefined_queries, \n4:mem_random_query, \n5:mem_random_queries, \n6:get_data_set_size \n7:Exits \nEnter your option:')
  
        #Take the input from the user to perform the required operation.
        if option == 7:
            break
        options[option](connection)

if __name__ == '__main__':
    main(sys.argv)
# [END all]
