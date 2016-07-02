import sys
import MySQLdb
import memcache
memc = memcache.Client(['127.0.0.1:11211'], debug=1);
try:
    conn = MySQLdb.connect (host = "mysqlone.co22szrummwy.us-west-2.rds.amazonaws.com",
							port="3306",
                            user = "Abhinavjoshi",
                            passwd = "rootroot",
                            db = "vehicledata")
except MySQLdb.Error, e:
     print "Error %d: %s" % (e.args[0], e.args[1])
     sys.exit (1)
popularfilms = memc.get('top5films')
if not popularfilms:
    cursor = conn.cursor()
    cursor.execute('select DMV_Plate from vehicledata.Vehicle_Insurance where Automobile_Insurance_code=135 ')
    rows = cursor.fetchall()
    memc.set('top5films',rows,165)
    print "Updated memcached with MySQL data"
else:
    print "Loaded data from memcached"
    for row in popularfilms:
        print "%s, %s" % (row[0])