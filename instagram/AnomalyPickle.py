#!/usr/bin/env python

import time
from datetime import datetime, timedelta
import pymysql as mdb
import pickle
import pandas as pd
import re

# read database info

with open('/home/ubuntu/instaNYC/db.pkl', 'rb') as handle:
  db_info = pickle.load(handle)

db = mdb.connect(user=db_info["user"], password=db_info["password"], host=db_info["host"], db=db_info["database"], charset='utf8')

# get yesterday's date
time.sleep(5)
# given_date = '2015-02-03'
given_date = (datetime.utcnow() - timedelta(hours=5) - timedelta(days=1)).date().strftime('%Y-%m-%d')
date_start = (datetime.strptime(given_date, '%Y-%m-%d') - timedelta(days=6)).date().strftime('%Y-%m-%d')

# finding anomalies for the given day
df = pd.read_sql('SELECT date_time, loc_id FROM events WHERE DATE(date_time) = "%s" AND type = "Anomaly" ORDER BY loc_id, date_time' % given_date, db)

# only cases that have more than half hour of anomaly
a = df.groupby('loc_id').apply(lambda g: len(g)>1)
ids = a[a == True].index.tolist()
df = df[df['loc_id'].isin(ids)]
df = df.sort(['loc_id', 'date_time'])

# getting the first and last time point for all loc_ids
first = df.groupby('loc_id').first()
last = df.groupby('loc_id').last()
df = first.append(last).reset_index()

# time should be converted to UTC, and extended to one hour before
events = {}

for id in ids:
    duration = df[df.loc_id == id]['date_time'].tolist()
    duration[0] = duration[0] + timedelta(hours=4)
    duration[1] = duration[1] + timedelta(hours=5)
    events[id] = [x.strftime('%Y-%m-%d %H:%M:%S') for x in duration]

# make the marker dictionary
# we get urls from all loc_ids with the same ID
marker = {}
for ID in events:
    with db:
        cur = db.cursor()
        cur.execute('SELECT loc_lat, loc_lon, loc_name, loc_id FROM top_places_nyc WHERE id = "%s"' % ID)
        results_2 = cur.fetchall()

        cur.execute('SELECT thumbnail_url FROM nyc_data WHERE loc_id IN (SELECT loc_id FROM top_places_nyc WHERE id = "%s") AND time > "%s" AND time < "%s" GROUP BY user' % (ID, events[ID][0], events[ID][1]))
        results_3 = cur.fetchall()

        cur.execute('SELECT date_time, counts FROM hourly_counts WHERE loc_id IN (SELECT loc_id FROM top_places_nyc WHERE id = "%s") AND DATE(date_time) > "%s" AND DATE(date_time) <= "%s"' % (ID, date_start, given_date))
        results_4 = cur.fetchall()

        marker[ID] = {'lat': float(results_2[0][0]), 'lon': float(results_2[0][1]),\
                     'event_type': 'Anomaly', 'name': re.sub('[.!,;]', '', results_2[0][2].encode("ascii")),\
                     'url': [x[0] for x in results_3][:18],\
                     't_h': [x[0].strftime("%Y-%m-%d %H:%M:%S") for x in results_4],\
                     'x_h': [x[1] for x in results_4]}

        # marker[ID] = {'lat': float(results_2[0][0]), 'lon': float(results_2[0][1]), \
        #              'event_type': 'Anomaly', 'name': re.sub('[.!,;]', '', results_2[0][2].encode("ascii")), \
        #              'url': [x[0] for x in results_3][:18]}

# we remove the locations with fewer than 10 photos
popped = []
for id in marker:
    if len(marker[id]['url']) < 10:
        popped.append(id)
        
for id in popped:
    marker.pop(id, None)

# pickling the files
filename = '/home/ubuntu/instaNYC/website/app/static/pickles/' + 'anomalies_' + given_date + '.pkl'
with open(filename, 'wb') as handle:
  pickle.dump(marker, handle)
