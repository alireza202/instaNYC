#!/usr/bin/env python

import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time

midnight = time(0, 0, 0)
morning = time(8, 59, 0)

Now = datetime.utcnow() - timedelta(hours=5)

if (Now.time() > midnight) & (Now.time() < morning):
	sys.exit()

import pymysql as mdb
con = mdb.connect('instagram.cyhrulrbvwbq.us-east-1.rds.amazonaws.com', 'root', 'mypassword', 'instagramdb')

time_start = datetime.utcnow() - timedelta(hours=1)
time_end = datetime.utcnow()

time_start = time_start.strftime('%Y-%m-%d %H:%M:%S')
time_end = time_end.strftime('%Y-%m-%d %H:%M:%S')


df = pd.read_sql('SELECT id, loc_id, time, user FROM nyc_data WHERE time >= "%s" AND time < "%s"' % (time_start, time_end), con)

# convert UTC to ET
df['time'] = df['time'].apply(lambda x: x - timedelta(hours=5))

loc_info = pd.read_sql("SELECT * FROM top_places_nyc", con)
loc_info.columns = ['loc_id', 'loc_name', 'loc_lat', 'loc_lon', 'ID']

df = pd.merge(df, loc_info, on='loc_id', how='inner')

# remove duplicated photos of a user at a given location
# we don't want to have a trending place because of one user's multiple uploads
grouped = df.groupby(['loc_id', 'user'])
df = grouped.apply(lambda row: row[row['time'] == row['time'].max()])
df.reset_index(inplace=True, drop=True)

# get the counts per hour
counts = df.groupby('ID').size().reset_index()
counts.columns = ['loc_id', 'counts']
counts = counts.set_index('loc_id')['counts'].to_dict()


Now = Now.replace(minute=0, second=0, microsecond=0)

# write to database
con = mdb.connect('localhost', 'root', '', 'instagram')
for key, value in counts.iteritems():
    cur = con.cursor()
    cur.execute('INSERT IGNORE INTO hourly_counts (date_time, loc_id, counts) \
    			VALUES ("%s", "%s", "%s");' % (Now, key, value))
