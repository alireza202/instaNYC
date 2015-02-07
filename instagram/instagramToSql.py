#!/usr/bin/env python

import os
import sys
import time
from datetime import datetime
import pandas as pd
import numpy as np

# for logging
from time import gmtime, strftime
now = strftime("%Y-%m-%d_%H-%M-%S", gmtime())

if not os.path.exists('logs'):
    os.makedirs('logs')


args = sys.argv[1:]
if args:
    duration = int(args[0])      # duration of running Instagram API
else:
    print "usage is ./script.py duration, where duration is a number in minutes."
    sys.exit()

import pymysql as mdb

try:
    # read database info

    with open('/home/ubuntu/instaNYC/db.pkl', 'rb') as handle:
      db_info = pickle.load(handle)

    db = mdb.connect(user=db_info["user"], password=db_info["password"], host=db_info["host"], db=db_info["database"], charset='utf8')
    
except:
    log = open('logs/log_' + now, 'a')
    log.write("Could not open the database.")
    log.write('\n')
    log.close()
    sys.exit()

try:
    with db:
        cur = db.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS nyc_data(\
            id VARCHAR(40) PRIMARY KEY,\
            time DATETIME NOT NULL,\
            loc_id INT NOT NULL,\
            lat DECIMAL(11, 9) NOT NULL,\
            lon DECIMAL(11, 9) NOT NULL,\
            tags TEXT NOT NULL,\
            likes INT NOT NULL,\
            type VARCHAR(6) NOT NULL,\
            user VARCHAR(255) NOT NULL,\
            filter VARCHAR(30) NOT NULL,\
            thumbnail_url MEDIUMTEXT NOT NULL,\
            standard_url MEDIUMTEXT NOT NULL)\
            ENGINE=MyISAM DEFAULT CHARSET=utf8")
except:
    log = open('logs/log_' + now, 'a')
    log.write("Could not create table.")
    log.write('\n')
    log.close()
    sys.exit()

from instagram.client import InstagramAPI

try:
    INSTAGRAM_CLIENT_ID = '61712f4e787b4897a05fafb4bb53094c'
    INSTAGRAM_CLIENT_SECRET = 'db556d55cdb0467da293089b8083fab0'
    api = InstagramAPI(client_id=INSTAGRAM_CLIENT_ID,
                       client_secret=INSTAGRAM_CLIENT_SECRET)
except:
    log = open('logs/log_' + now, 'a')
    log.write("Instagram API could not be defined.")
    log.write('\n')
    log.close()
    sys.exit()

recent_media = []

t_f = int(time.time()) + 60 * duration
while time.time() < t_f:
    t1 = int(time.time())
    time.sleep(5)
    t2 = int(time.time())
    
    try:
        recent_media.extend(api.media_search(lat=40.742241, lng=-73.98895, distance=5000,
                                             min_timestamp = t1, max_timestamp = t2))
    except:
        log = open('logs/log_' + now, 'a')
        log.write("Could not get data from Instagram.")
        log.write('\n')
        log.close()
        sys.exit()
    
    time.sleep(1)

if len(recent_media) == 0:
    log = open('logs/log_' + now, 'a')
    log.write("No recent media were pulled.")
    log.write('\n')
    log.close()
    sys.exit()


id = []
for media in recent_media:
    id.append(media.id)


#print "recent_media size is: ", len(recent_media)

# creating the dataframe
columns = ['time', 'loc_id', 'lat', 'lon', 'tags', 'likes', 'type', 
           'user', 'filter', 'thumbnail_url', 'standard_url']
index = set(id)
df = pd.DataFrame(index = index, columns = columns)

try:
    # filling it with the data
    for media in recent_media:
        if hasattr(media, 'tags'):
            tags = [x.name for x in media.tags]
        else:
            tags = ['notags']
        df.loc[media.id] = pd.Series({'time':media.created_time, 
                                      'loc_id':media.location.id, 
                                      'lat':media.location.point.latitude,
                                      'lon':media.location.point.longitude,
                                      'tags':tags,
                                      'likes':media.like_count,
                                      'type':media.type,
                                      'user':media.user.username,
                                      'filter':media.filter,
                                      'thumbnail_url':media.get_thumbnail_url(),
                                      'standard_url':media.get_standard_resolution_url()})
except:
    log = open('logs/log_' + now, 'a')
    log.write("Could not fill the dataframe with data.")
    log.write('\n')
    log.close()
    sys.exit()

try:
    with db:
        loc_info = pd.read_sql("SELECT loc_id FROM top_places_nyc", db)
        loc_info = loc_info['loc_id'].tolist()
except:
    log = open('logs/log_' + now, 'a')
    log.write("Could not read loc_info from server.")
    log.write('\n')
    log.close()
    sys.exit()

try:
    # formatting
    df['tags'] = df['tags'].apply(lambda x: ', '.join(x))
    df['tags'] = df['tags'].apply(lambda x: x.encode('ascii', 'ignore'))
    df.reset_index(level=0, inplace=True)
    df = df.rename(columns={'index':'id'})
    df['loc_id'] = df['loc_id'].astype('int')
    # only top_places are written to db
    df = df[df['loc_id'].isin(loc_info)]
except:
    log = open('logs/log_' + now, 'a')
    log.write("Could not format the dataframe properly.")
    log.write('\n')
    log.close()
    sys.exit()


try:
    for l in map(list, df.values):
        with db:
            cur = db.cursor()
            cur.execute('INSERT IGNORE INTO nyc_data \
                        (id, time, loc_id, lat, lon, tags, likes, type, user, filter, thumbnail_url, standard_url) \
                        VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s");' % tuple(l))
except:
    log = open('logs/log_' + now, 'a')
    log.write("Could not insert data into database.")
    log.write('\n')
    log.close()
    sys.exit()
                
db.close()
