# Import the SDK - boto
import sys
import boto
from boto.s3.key
import time
import csv
import MySQLdb
import random


_BUCKET_NAME = 'cloud6331-0809'
_CSV_FILE = 'Vehicle.csv'

#DB connection params
__RDS_HOST_ADDR = 'mysqlone.co22szrummwy.us-west-2.rds.amazonaws.com'
_RDS_PORT = 3306
_DATABASE_NAME = 'consumer'
_TABLE_NAME = 'T_CONSUMER_COMPLAINTS'
def upload_file(connection):

    try:
        bucket_exist = connection.lookup(_BUCKET_NAME)
        if bucket_exist is None:
            connection.create_bucket(_BUCKET_NAME)

        # Upload file part
        file_name = raw_input('Enter file name to be uploaded to Amazon S3\n')
        bucket = connection.get_bucket(_BUCKET_NAME)
        file.set_contents_from_filename(file_name)
        end = time.time()
        duration = end - start
        print('Upload took {} seconds'.format(duration))
        _CSV_FILE = file_name
    except Exception, exe:
        print exe
        raise

def get_db():
    print 'Establishing db connection to - {}'.format(_DATABASE_NAME)
    db = MySQLdb.connect(host=_RDS_HOST_ADDR, port=_RDS_PORT, db=_DATABASE_NAME, user='application', passwd='application')
    return db

# Creates table as per the name given, if not already created     
def create_table(db):
    print 'Entering create_table function'
    cursor = None
    try:
        create__query = 'CREATE TABLE IF NOT EXISTS {}.{}(\
                        COMPLAINT_ID INT NOT NULL,\
                        PRODUCT VARCHAR(255) NOT NULL,\
                        SUB_PRODUCT VARCHAR(255),\
                        ISSUE VARCHAR(255),\
                        SUB_ISSUE VARCHAR(255),\
                        STATE VARCHAR(10),\
                        ZIP_CODE INT,\
                        SUBMIT_TYPE VARCHAR(50),\
                        DATE_RECEIVED DATE,\
                        DATE_SENT_TO_COMPANY DATE,\
                        COMPANY VARCHAR(255),\
                        COMPANY_RESPONSE VARCHAR(255),\
                        TIMELY_RESPONSE VARCHAR(10),\
                        CONSUMER_DISPUTED VARCHAR(10),\
                        PRIMARY KEY(COMPLAINT_ID))'.format(_DATABASE_NAME, _TABLE_NAME, _TABLE_NAME)
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
                COMPLAINT_ID, PRODUCT, SUB_PRODUCT, ISSUE, SUB_ISSUE, STATE, \
                ZIP_CODE, SUBMIT_TYPE, DATE_RECEIVED, DATE_SENT_TO_COMPANY, \
                COMPANY, COMPANY_RESPONSE, TIMELY_RESPONSE, CONSUMER_DISPUTED)\
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
                %s, %s, %s, %s)'.format(_DATABASE_NAME, _TABLE_NAME)
        
        csv_file =  open(_CSV_FILE, 'rb')
        reader = csv.DictReader(csv_file)

        # Reads each row in CSV, creates an array of 1000 entries and does batch insert
        counter = 0
        entries_list = []
        cursor = db.cursor()
        for row in reader:
            entries_list.append((row['Complaint ID'], row['Product'], row['Sub-product'],
                                row['Issue'], row['Sub-issue'], row['State'], row['ZIP code'], row['Submitted via'],
                                row['Date received'], row['Date sent to company'], row['Company'], row['Company response'],
                                row['Timely response?'], row['Consumer disputed?']))
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
            csvfile.close()

def dataimport(connection):
    bucket = connection.get_bucket(_BUCKET_NAME)
    file = Key(bucket)
    db = None
    start = time.time()
    try:
        file.key = _CSV_FILE
        file.get_contents_to_filename(_CSV_FILE)
        db = get_db()
        create_table(db)
        truncate_table(db)
        populate_db(db)
        
    except MySQLdb.Error, msqe:
        raise
    except Exception, e:
        raise
    finally:
        if db and db.open:
            dbclose()
    end = time.time()
    duration = end - start
    print 'Importing data from S3 to RDS took {} seconds'.format(duration)

def rand_queries(connection):
    print 'Entering random_queries function'
    db = None
    try:
        db = getdb()
        #Run random queries 1000, 5000, 20000 times
        duration = random_query(db, 1000)
        print('Took {} seconds to do 1000 random queries'.format(duration))
        duration = random_query(db, 5000)
        print('Took {} seconds to do 5000 random queries'.format(duration))
        duration = random_query(db, 20000)
        print('Took {} seconds to do 20000 random queries'.format(duration))
        
    except MySQLdb.Error, msqe:
        raise
    except Exception, e:
        raise
    finally:
        if db and db.open:
            dbclose()

def random_query(db, query_count):
    # Parameter used to randomize the query
    print ('Starting random querying for {} entries, param value - {}'.format(query_count, zipcode))
    cursor = None
    counter = 0
    start = time.time()
    try:
        random_query = 'SELECT COUNT(1) FROM  WHERE  %s'.format(_DATABASE_NAME, _TABLE_NAME)
        cursor = db.cursor()
        print 'Got cursor'
        while (counter < query_count):
            #print 'Doing query - {}'.format(counter)
            cursor.execute(random_query, (zipcode))
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
    s3_connection = botoconnect_s3()
    
    #Store the option and name of the function as the key value pair in the dictionary.
    options = {1: upload_file, 2: dataimport, 3: random_queries}
    while True:
        option = input('Select the operation to be run [1:Upload data file, 2:Import data to RDS, 3:Run random queries, 4:Fetch subset tuples, 5:Exit]:\n')
  
        #Take the input from the user to perform the required operation.
        if option == 0:
            break
        options[option](s3_connection)

if __name__ == '__main__':
    main(sys.argv)
# [END all]
