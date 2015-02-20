#!/usr/bin/env python

import pymysql as mdb
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, time
import pandas as pd
import numpy as np
import pickle

# read database info

with open('/home/ubuntu/instaNYC/db.pkl', 'rb') as handle:
# with open('../db.pkl', 'rb') as handle:
  db_info = pickle.load(handle)

db = mdb.connect(user=db_info["user"], password=db_info["password"], host=db_info["host"], db=db_info["database"], charset='utf8')

date_start = (datetime.utcnow() - timedelta(days=15)).date()
date_end = (datetime.utcnow() - timedelta(days=1)).date()

date_start = date_start.strftime('%Y-%m-%d')
date_end = date_end.strftime('%Y-%m-%d')


df = pd.read_sql('SELECT DATE(time) As thedate, COUNT(*) AS counts \
				 FROM nyc_data GROUP BY thedate \
				 HAVING thedate BETWEEN "%s" AND "%s"' % (date_start, date_end), db)

df['thedate'] = df['thedate'].apply(lambda x: x.strftime('%d %b'))

ax = df.plot(x='thedate', y='counts', rot = 45, legend = False)

fig = ax.get_figure()
fig.savefig('../website/app/static/monitor/monitor.png')

