#!/usr/bin/env python

import sys
import pandas as pd
import numpy as np
import pymysql as mdb
from datetime import datetime, timedelta, time

midnight = time(0, 0, 0)
morning = time(8, 59, 0)

Now = datetime.utcnow() - timedelta(hours=5)

if (Now.time() > midnight) & (Now.time() < morning):
	sys.exit()

# read database info

with open('../db.pkl', 'rb') as handle:
  db_info = pickle.load(handle)

db = mdb.connect(user=db_info["user"], password=db_info["password"], host=db_info["host"], db=db_info["database"], charset='utf8')


time_start = datetime.utcnow() - timedelta(hours=1)
time_end = datetime.utcnow()

time_start = time_start.strftime('%Y-%m-%d %H:%M:%S')
time_end = time_end.strftime('%Y-%m-%d %H:%M:%S')


df = pd.read_sql('SELECT id, loc_id, time, user FROM nyc_data WHERE time >= "%s" AND time < "%s"' % (time_start, time_end), db)

# convert UTC to ET
df['time'] = df['time'].apply(lambda x: x - timedelta(hours=5))

loc_info = pd.read_sql("SELECT * FROM top_places_nyc", db)
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
db = mdb.connect('localhost', 'root', '', 'instagram')
for key, value in counts.iteritems():
    cur = db.cursor()
    cur.execute('INSERT IGNORE INTO hourly_counts (date_time, loc_id, counts) \
    			VALUES ("%s", "%s", "%s");' % (Now, key, value))
