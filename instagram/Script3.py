#!/usr/bin/env python

import os
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time

now = datetime.utcnow() - timedelta(hours=5)
now = now.strftime("%Y-%m-%d_%H-%M-%S")
if not os.path.exists('logs'):
    os.makedirs('logs')

try:
    import pymysql as mdb
    db = mdb.connect('instagram.cyhrulrbvwbq.us-east-1.rds.amazonaws.com', 'root', 'mypassword', 'instagramdb')
except:
    log = open('logs/log_s3_' + now, 'a')
    log.write("Could not open the database.")
    log.write('\n')
    log.close()
    sys.exit()

# getting start and end times, taking into effect UTC timezone
# time_start is 1 hour before
# time_end is now

time_start = datetime.utcnow() - timedelta(hours=1)
time_end = datetime.utcnow()

time_start = time_start.strftime('%Y-%m-%d %H:%M:%S')
time_end = time_end.strftime('%Y-%m-%d %H:%M:%S')

# read the last hour data from server
try:
    df = pd.read_sql('SELECT id, loc_id, time, user, thumbnail_url FROM nyc_data WHERE time >= "%s" AND time < "%s"' % (time_start, time_end), db)
except:
    log = open('logs/log_s3_' + now, 'a')
    log.write("Could not read df from server.")
    log.write('\n')
    log.close()
    sys.exit()

# convert UTC to ET
df['time'] = df['time'].apply(lambda x: x - timedelta(hours=5))

try:
    loc_info = pd.read_sql("SELECT * FROM top_places_nyc", db)
    loc_info.columns = ['loc_id', 'loc_name', 'loc_lat', 'loc_lon', 'ID']
except:
    log = open('logs/log_s3_' + now, 'a')
    log.write("Could not read loc_info from server.")
    log.write('\n')
    log.close()
    sys.exit()

try:
    df = pd.merge(df, loc_info, on='loc_id', how='inner')
except:
    log = open('logs/log_s3_' + now, 'a')
    log.write("Could not merge df and loc_info.")
    log.write('\n')
    log.close()
    sys.exit()

# remove duplicated photos of a user at a given location
# we don't want to have a trending place because of one user's multiple uploads
try:
    grouped = df.groupby(['loc_id', 'user'])
    df = grouped.apply(lambda row: row[row['time'] == row['time'].max()])
    df.reset_index(inplace=True, drop=True)
except:
    log = open('logs/log_s3_' + now, 'a')
    log.write("Could not remove duplicate photos.")
    log.write('\n')
    log.close()
    sys.exit()

counts = df.groupby('ID').size().reset_index()
counts.columns = ['loc_id', 'counts']

# read the quartiles from last 5 days
try:
    todayET = datetime.today() - timedelta(hours=5)
    todayET = todayET.date()
    quartiles = pd.read_sql('SELECT loc_id, med, q25, q75, anomaly FROM quartiles WHERE DATE(date)= "%s"' % todayET, db)
except:
    log = open('logs/log_s3_' + now, 'a')
    log.write("Could not read quartiles from server.")
    log.write('\n')
    log.close()
    sys.exit()

try:
    results = pd.merge(counts, quartiles, on='loc_id')
    results = results.set_index('loc_id')
except:
    log = open('logs/log_s3_' + now, 'a')
    log.write("Could not merge counts and quartiles.")
    log.write('\n')
    log.close()
    sys.exit()

results['isLow'] = results['counts'] < results['q25']
results['isHigh'] = results['counts'] > results['q75']
results['isAnomaly'] = results['counts'] > results['anomaly']

# keeping the events
results = results[results.isLow | results.isHigh | results.isAnomaly]
events = dict()
for id in results.index:
    if results.loc[id, 'isAnomaly']:
        events[id] = ('Anomaly', results.loc[id, 'counts'], results.loc[id, 'med'], results.loc[id, 'anomaly'])
    elif results.loc[id, 'isLow']:
        events[id] = ('Low', results.loc[id, 'counts'], results.loc[id, 'med'], results.loc[id, 'anomaly'])
    else:
        events[id] = ('High', results.loc[id, 'counts'], results.loc[id, 'med'], results.loc[id, 'anomaly'])

# writing the results to database
try:
    Now = datetime.utcnow() - timedelta(hours=5)
    with db:
        cur = db.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS events(\
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,\
            date_time DATETIME NOT NULL,\
            loc_id INT NOT NULL,\
            type VARCHAR(10) NOT NULL,\
            counts INT NoT NULL,\
            med INT NOT NULL,\
            mad INT NOT NULL) \
            ENGINE=MyISAM DEFAULT CHARSET=utf8")
        
        for key, value in events.iteritems():
            cur = db.cursor()
            cur.execute('INSERT IGNORE INTO events \
                        (date_time, loc_id, type, counts, med, mad) \
                        VALUES ("%s", "%s", "%s", "%s", "%s", "%s");' % \
                        (Now, key, value[0], value[1], value[2], value[3]))
except:
    log = open('logs/log_s3_' + now, 'a')
    log.write("Could not write data to database.")
    log.write('\n')
    log.close()
    sys.exit()



