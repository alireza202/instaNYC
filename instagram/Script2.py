#!/usr/bin/env python

import os
import time
import pandas as pd
import numpy as np
import sys
import pymysql as mdb
from datetime import datetime, timedelta, time

def noZero(x):
    return filter(lambda a: a != 0, x)


now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
if not os.path.exists('logs'):
    os.makedirs('logs')

try:
    # read database info

    with open('/home/ubuntu/instaNYC/db.pkl', 'rb') as handle:
      db_info = pickle.load(handle)

    db = mdb.connect(user=db_info["user"], password=db_info["password"], host=db_info["host"], db=db_info["database"], charset='utf8')
except:
    log = open('logs/log_s2_' + now, 'a')
    log.write("Could not open the database.")
    log.write('\n')
    log.close()
    sys.exit()

# getting start and end times, taking into effect UTC timezone
# time_start is 7 days before at 14:00 UTC, which is 9:00 ET
# time_end is the same day at 4:59 UTC, which is 23:59 ET the previous day
num_days = 7
time_start = datetime.combine(datetime.today().date() - timedelta(days=num_days), time(14, 0))
time_end = datetime.combine(datetime.today().date(), time(4, 59))

time_start = time_start.strftime('%Y-%m-%d %H:%M:%S')
time_end = time_end.strftime('%Y-%m-%d %H:%M:%S')

# reading in the last 7 days of data from server
try:
    df = pd.read_sql('SELECT id, loc_id, time, user \
                    FROM nyc_data WHERE time >= "%s" AND time < "%s"' % (time_start, time_end), db)
except:
    log = open('logs/log_s2_' + now, 'a')
    log.write("Could not read df from database.")
    log.write('\n')
    log.close()
    sys.exit()

# convert UTC to ET
df['time'] = df['time'].apply(lambda x: x - timedelta(hours=5))

# reading in the location info of the top 100 aggregated locations
try:
    loc_info = pd.read_sql("SELECT * FROM top_places_nyc", db)
    loc_info.columns = ['loc_id', 'loc_name', 'loc_lat', 'loc_lon', 'ID']
except:
    log = open('logs/log_s2_' + now, 'a')
    log.write("Could not read loc_info from the database.")
    log.write('\n')
    log.close()
    sys.exit()

# merge location info with df. limit to photos taken at top 100 IDs
try:
    df = pd.merge(df, loc_info, on='loc_id', how='inner')
except:
    og = open('logs/log_s2_' + now, 'a')
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
    log = open('logs/log_s2_' + now, 'a')
    log.write("Could not remove duplicate photos of users.")
    log.write('\n')
    log.close()
    sys.exit()

IDs = sorted(list(set(df['ID'])))

# making the time-binned matrix
time_start = datetime.combine(datetime.today().date() - timedelta(days=num_days), time(9, 0))
time_end = datetime.combine(datetime.today().date() - timedelta(days=1), time(23, 59))

myIndex = pd.date_range(time_start.strftime("%Y-%m-%d %H:%M:%S"), time_end.strftime("%Y-%m-%d %H:%M:%S"), freq="1H")
myIndex = myIndex[myIndex.indexer_between_time('9:00','23:59')]

# converting myIndex to datetime format
date_time = []
for i in range(myIndex.shape[0]):
    ts = (myIndex.values[i] - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')
    date_time.append(datetime.fromtimestamp(ts))

# binned is hourly count of photos. rows are locations, columns are hours
binned = pd.DataFrame(0, columns=IDs, index=date_time)

mt = df[df['time'].apply(lambda t: t.hour >= 9)]
mt = mt[(mt['ID'].isin(IDs)) & (mt['time'] > time_start) & (mt['time'] < time_end)]
mt = mt[['time', 'ID']]
mt = mt.sort('time')

# try:
for i in range(mt.shape[0]):
    binned.loc[mt.iloc[i, 0].replace(second=0, minute=0), mt.iloc[i, 1]] += 1
# except:
#     log = open('logs/log_s2_' + now, 'a')
#     log.write("Could not bin the df.")
#     log.write('\n')
#     log.close()
#     sys.exit()


# remove weekends and times between 0:00 and 8:59
try:
    bins = [(x.weekday() in (5, 6)) or (x.hour in range(9)) for x in binned.index.tolist()]
    binned = binned.drop(binned.index[bins])
except:
    log = open('logs/log_s2_' + now, 'a')
    log.write("Could not drop unwanted times.")
    log.write('\n')
    log.close()
    sys.exit()

# finding median, quartiles, and outlier thresholds
results = pd.DataFrame(0, index = IDs, columns = ['med', 'q25', 'q75', 'anomaly'])

for id in IDs:    
    x = noZero(binned[id])
    results.loc[id, 'med'] = np.ceil(np.median(x))
    results.loc[id, 'q25'] = np.floor(np.percentile(x, 25))
    results.loc[id, 'q75'] = np.ceil(np.percentile(x, 75))
    results.loc[id, 'anomaly'] = np.ceil(np.percentile(x, 75) + 1.5 * (np.floor(np.percentile(x, 75)) - np.floor(np.percentile(x, 25))))
    if results.loc[id, 'anomaly'] < 10:
        results.loc[id, 'anomaly'] = 10


# writing the results to database
# db = mdb.connect('localhost', 'root', '', 'instagram')
try:
    with db:
        cur = db.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS quartiles(\
                    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,\
                    date DATETIME NOT NULL,\
                    loc_id INT NOT NULL,\
                    med INT NOT NULL,\
                    q25 INT NOT NULL,\
                    q75 INT NOT NULL,\
                    anomaly INT NOT NULL)\
                    ENGINE=MyISAM DEFAULT CHARSET=utf8")
        
        for id in IDs:
            cur = db.cursor()
            cur.execute('INSERT IGNORE INTO quartiles(date, loc_id, med, q25, q75, anomaly) \
                        VALUES ("%s", "%s", "%s", "%s", "%s", "%s");' % \
                        (datetime.now().date(), id, results.loc[id, 'med'], results.loc[id, 'q25'], results.loc[id, 'q75'], results.loc[id, 'anomaly']))
except:
    log = open('logs/log_s2_' + now, 'a')
    log.write("Could not write to server.")
    log.write('\n')
    log.close()
    sys.exit()
