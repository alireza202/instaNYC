#!/usr/bin/env python

from __future__ import division
import sys
import time
from datetime import datetime, timedelta
import pymysql as mdb
import pickle
import pandas as pd
import numpy as np
import re
from operator import itemgetter
from scipy.stats import norm

def clean_tags(tags):
    
    tmp = [y.encode('ascii') for x in tags for y in x if y.encode('ascii') != 'notags']
    tmp = [x.split(', ') for x in tmp]
    
    return [y for x in tmp for y in x]

def tag_freq(l):
    
    a.sort()
    y = dict()
    
    for i, element in enumerate(l):
        if element in y:
            y[element] += 1
        else:
            y[element] = 1
    
    for key in y:
        y[key] /= len(l)
    
    return y

def top_tags(d):
    
    return dict(sorted(d.iteritems(), key=itemgetter(1), reverse=True)[:5])

def propotion_test(a, b):
    # a and b are tuples with (p, n) where p is proportion, and n is the total number
    p = (a[0] * a[1] + b[0] * b[1])/ (a[1] + b[1])
    se = np.sqrt(p * (1 - p) * (1/a[1] + 1/b[1]))
    z = (a[0] - b[0]) / se
    
    return norm.sf(z)*2

def hashtag_tester(hashtag, anomaly, all_tags):
    
    a = (anomaly[hashtag], len(anomaly))
    b = (all_tags[hashtag], len(all_tags))
    return propotion_test(a, b)



# read database info
# with open('../db.pkl', 'rb') as handle:
with open('/home/ubuntu/instaNYC/db.pkl', 'rb') as handle:
  db_info = pickle.load(handle)

db = mdb.connect(user=db_info["user"], password=db_info["password"], host=db_info["host"], db=db_info["database"], charset='utf8')
cur = db.cursor()


time.sleep(5)

try:
  given_date = sys.argv[1]
except:
  # get yesterday's date
  given_date = (datetime.utcnow() - timedelta(hours=5) - timedelta(days=1)).date().strftime('%Y-%m-%d')

date_start = (datetime.strptime(given_date, '%Y-%m-%d') - timedelta(days=6)).date().strftime('%Y-%m-%d')

# finding anomalies for the given day
df = pd.read_sql('SELECT date_time, loc_id FROM events WHERE DATE(date_time) = "%s" AND type = "Anomaly" ORDER BY loc_id, date_time' % given_date, db)

# only cases that have more than half hour of anomaly
a = df.groupby('loc_id').apply(lambda g: len(g)>1)

# check if df is empty
if df.shape[0] == 0:
  filename = '/home/ubuntu/instaNYC/website/app/static/pickles/' + 'anomalies_' + given_date + '.pkl'
  with open(filename, 'wb') as handle:
    pickle.dump('empty', handle)
  sys.exit()


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

# defining the day by UTC time
start_time = datetime.strptime(given_date, '%Y-%m-%d') + timedelta(hours=14)
end_time = datetime.strptime(given_date, '%Y-%m-%d') + timedelta(hours=29)

# finding tag frequency for the whole day
cur.execute('SELECT tags FROM nyc_data WHERE time >= "%s" AND time < "%s"' % (start_time, end_time))
all_tags = cur.fetchall()
all_tags = clean_tags(all_tags)
all_tags = tag_freq(all_tags)

# make the marker dictionary
# we get urls from all loc_ids with the same ID
marker = {}
for ID in events:
    cur.execute('SELECT loc_lat, loc_lon, loc_name, loc_id FROM top_places_nyc WHERE id = "%s"' % ID)
    results_2 = cur.fetchall()

    cur.execute('SELECT thumbnail_url FROM nyc_data WHERE loc_id IN (SELECT loc_id FROM top_places_nyc WHERE id = "%s") \
                AND time > "%s" AND time < "%s" GROUP BY user' % (ID, events[ID][0], events[ID][1]))
    results_3 = cur.fetchall()

    cur.execute('SELECT date_time, counts FROM hourly_counts WHERE loc_id IN (SELECT loc_id FROM top_places_nyc WHERE id = "%s") \
                AND DATE(date_time) > "%s" AND DATE(date_time) <= "%s"' % (ID, date_start, given_date))
    results_4 = cur.fetchall()

    cur.execute('SELECT tags FROM nyc_data WHERE loc_id IN (SELECT loc_id FROM top_places_nyc WHERE id = "%s") \
                AND time > "%s" AND time < "%s" GROUP BY user' % (ID, events[ID][0], events[ID][1]))
    anomaly = cur.fetchall()
    anomaly = clean_tags(anomaly)
    anomaly = tag_freq(anomaly)
    top_tags_anomaly = top_tags(anomaly)
    
    hashtags = []
    for hashtag in top_tags_anomaly:
        try:
          p_val = hashtag_tester(hashtag, anomaly, all_tags)
        except:
          p_val = 1
        if p_val < 0.05 and hashtag:
            hashtags.append((hashtag, p_val))
            
    # sort the list of tuples by p_val
    hashtags = sorted(hashtags, key=lambda x: x[1])
    hashtags = [x[0] for x in hashtags]

    the_day_after = (datetime.strptime(given_date, '%Y-%m-%d') + timedelta(days=1)).date().strftime("%Y-%m-%d")
    
    marker[ID] = {'lat': float(results_2[0][0]), 'lon': float(results_2[0][1]),\
                 'event_type': 'Anomaly',\
                 'name': re.sub('[.!,;]', '', results_2[0][2].encode("ascii")),\
                 'url': [x[0] for x in results_3][:18],\
                 't_h': [x[0].strftime("%Y-%m-%d %H:%M:%S") for x in results_4],\
                 'x_h': [x[1] for x in results_4],\
                 'hashtags': hashtags,\
                 'daterange': [given_date, the_day_after]}


# we remove the locations with fewer than 10 photos
popped = []
for id in marker:
    if len(marker[id]['url']) < 10:
        popped.append(id)
        
for id in popped:
    marker.pop(id, None)

# pickling the files
# filename = '../website/app/static/pickles/' + 'anomalies_' + given_date + '.pkl'
filename = '/home/ubuntu/instaNYC/website/app/static/pickles/' + 'anomalies_' + given_date + '.pkl'
with open(filename, 'wb') as handle:
  pickle.dump(marker, handle)
