#Name: Abhinav Joshi
#Course Number: CSE 6331 Section 004
#Lab Number: 2 Part 2
'''Copyright (c) 2015 HG,DL,UTA
   Python program runs on Google AppEngine '''

import cgi
import webapp2
from google.appengine.ext.webapp.util import run_wsgi_app

import MySQLdb
import os
import jinja2
import csv
import cloudstorage as gcs


default_retry_params = gcs.RetryParams(initial_delay=0.2,
                                      max_delay=5.0,
                                      backoff_factor=2,
                                      max_retry_period=15)
gcs.set_default_retry_params(default_retry_params)

# Configure the Jinja2 environment.
JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)

# Define your production Cloud SQL instance information.
_INSTANCE_NAME = 'vehicledata'
_BUCKET_NAME = 'vehicledata'
_CSV_FILE = 'TLC_Vehicle_Insurance.csv'
#https://cloud.google.com/appengine/docs/python/gettingstartedpython27/usingdatastore refered this code        
class MainPage(webapp2.RequestHandler):
    def get(self):

        DATABASES = {
        'default': {
            'ENGINE': 'MySQL 5.6.22 ',
            'NAME': 'vehicledata',
            'USER': 'Abhinavjoshi',
            'PASSWORD': 'rootroot',
            'HOST': 'vehicledata.c53i3bit4kf6.us-west-2.rds.amazonaws.com',
            'PORT': '3306',
        }
    }
        # Create table and populate 
        self.createTable(db)
        self.truncateData(db)
        self.populateData(db)
        
        
        # Get weekly earth quake data for predefined values
        magnitudeTwo = self.getData(db, '2', 1)
        magnitudeThree = self.getData(db, '3', 1)
        magnitudeFour = self.getData(db, '4', 1)
        magnitudeFive = self.getData(db, '5', 1)
        magnitudeGreaterFive = self.getData(db, 'gt5', 2)
    
        # After usage closing the connection to DB
        db.close()
        
        variables = {'magnitudeTwo': magnitudeTwo,
                     'magnitudeThree': magnitudeThree,
                     'magnitudeFour': magnitudeFour,
                     'magnitudeFive': magnitudeFive,
                     'magnitudeGreaterFive': magnitudeGreaterFive}
        template = JINJA_ENVIRONMENT.get_template('main.html')
        self.response.write(template.render(variables))

    def getData(self, db, magnitude, query_type):
        cursor = db.cursor()
        if query_type == 1:
            cursor.execute('SELECT WEEK(time) WEEK, COUNT(1) NO FROM \
                            vehicledata.Data WHERE mag<=%s and mag>%s GROUP BY WEEK(time)', (magnitude, int(magnitude)-1))
        else:
            cursor.execute('SELECT WEEK(time) WEEK, COUNT(1) NO FROM vehicledata.Data WHERE mag>%s GROUP BY WEEK(time)', ('5'))

        weeklyData = [];
        for row in cursor.fetchall():
            weeklyData.append(dict([('week',row[0]),
                                 ('count',row[1])
                                 ]))
        return weeklyData
        

    def createTable(self, db):
        createQuery = 'CREATE TABLE IF NOT EXISTS vehicledata.Data(\
                        TLC_License_Type char(10) NOT NULL,\
                        TLC_License_Number varchar(10) NOT NULL,\
                        DMV_Plate varchar(15) NOT NULL,\
                        VIN varchar(10) NOT NULL,\	
                        Automobile_Insurance_Code INT(5) NOT NULL,\
                        Automobile_Insurance_Policy_Number VARCHAR(25),\
                        Vehicle_Owner_Name VARCHAR(30),\
                        Affiliated_Base_or_Taxi_Agent_or_Fleet_License_Number varchar(15),\
                        PRIMARY KEY(VIN))'
        cursor = db.cursor()
        cursor.execute(createQuery)
        db.commit()

    def truncateData(self, db):
		
        cursor = db.cursor()
        truncateQuery = '''TRUNCATE vehicledata.Data''';
        cursor.execute(truncateQuery)
        db.commit()
        
    def populateData(self, db):
        bucket = '/' + _BUCKET_NAME
        fileName = bucket + '/' + _CSV_FILE
		#https://docs.python.org/2/library/csv.html This code is referred from this website, as a reference for reading the csv file
        gcsFile = gcs.open(fileName)
        reader = csv.DictReader(gcsFile)
        insertQuery = 'INSERT INTO vehicledata.Data(\
                        time, latitude, longitude, depth, mag, magType,\
                        nst, gap, dmin, rms, net, id, updated, place, type)\
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
                        %s, %s, %s, %s, %s)'
        counter = 0
        cursor = db.cursor()
        entriesList = []
        for row in reader:
            entriesList.append((row['time'], row['latitude'], row['longitude'],
                           row['depth'], row['mag'], row['magType'], row['nst'], row['gap'],
                           row['dmin'], row['rms'], row['net'], row['id'], row['updated'],
                           row['place'], row['type']))
            counter = counter + 1
            if counter%500 == 0:
                cursor.executemany(insertQuery, entriesList)
                db.commit()
                del entriesList[:]

        if counter%500 != 0:
            cursor.executemany(insertQuery, entriesList)
            db.commit()
            del entriesList[:]
            
        cursor.close()    
        gcsFile.close()
        

app = webapp2.WSGIApplication([
    webapp2.Route('/', handler=MainPage),
], debug=True)
