#Name: Abhinav Joshi
#Course Number: CSE 6331 Section 004
#Lab Number: 2 Upload File Module
'''Copyright (c) 2015 HG,DL,UTA
   Python program runs on local host, uploads, downloads, encrypts local files to google.
   Please use python 2.7.X, pycrypto 2.6.1 and Google Cloud python module '''

#import statements.
import argparse
import httplib2
import os
import sys
import json
import time
import datetime
import io
import tinys3


#Name of your Amazon bucket.
BUCKET_NAME = 'vehicledata' 

#AWS ACCESS KEYS DETAILS
AWS_ACCESS_KEY_ID='AKIAJDA26BP7XE7IY3EA'
AWS_SECRET_ACCESS_KEY='9k6IzCtN6lfPgBOIC72qvc+0oEnqnn/rBhyXGlfK'

# Creating a simple connection
conn = tinys3.Connection(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,default='vehicledata')

# Uploading a single file
print 'Hello'
f = open('credentials.csv','rb')
conn.upload('credentials.csv',f)
conn = tinys3.Connection(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,tls=True)