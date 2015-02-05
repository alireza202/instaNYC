import sys, logging
logging.basicConfig(stream = sys.stderr)

from flask import render_template, request, jsonify
from app import app
from datetime import datetime, timedelta
import pymysql as mdb
import json
import re, os
import pandas as pd
import numpy as np
from time import mktime
import pickle

class MyEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            return int(mktime(obj.timetuple()))

        return json.JSONEncoder.default(self, obj)

db = mdb.connect(user="root", password="mypassword", host="instagram.cyhrulrbvwbq.us-east-1.rds.amazonaws.com", db="instagramdb", charset='utf8')

@app.route('/')
@app.route('/index')
def index():
    hours_to_show = 2

    date_start = (datetime.utcnow() - timedelta(hours=5, days=6)).date()
    time_start = datetime.utcnow() - timedelta(hours=5, minutes=15)
    time_end = datetime.utcnow() - timedelta(hours=5)

    # get current markers
    marker = {}

    cur = db.cursor()
    cur.execute('SELECT loc_id, type FROM events WHERE date_time >= "%s" AND date_time < "%s"' % (time_start, time_end))
    results = cur.fetchall()

    for i, (ID, event_type) in enumerate(results):
        cur = db.cursor()
        cur.execute('SELECT loc_lat, loc_lon, loc_name, loc_id FROM top_places_nyc WHERE id = "%s"' % ID)
        results_2 = cur.fetchall()

        cur.execute('SELECT thumbnail_url FROM nyc_data WHERE loc_id IN (SELECT loc_id FROM top_places_nyc WHERE id = "%s") AND time > "%s" GROUP BY user ORDER BY time DESC' % (ID, datetime.utcnow() - timedelta(hours=hours_to_show)))
        results_3 = cur.fetchall()

        cur.execute('SELECT date_time, counts FROM hourly_counts WHERE loc_id IN (SELECT loc_id FROM top_places_nyc WHERE id = "%s") AND DATE(date_time) > "%s"' % (ID, date_start))
        results_4 = cur.fetchall()

        marker[ID] = {'lat': float(results_2[0][0]), 'lon': float(results_2[0][1]),\
                     'event_type': event_type, 'name': re.sub('[.!,;]', '', results_2[0][2].encode("ascii")),\
                     'url': [x[0] for x in results_3][:18],\
                     't_h': [x[0].strftime("%Y-%m-%d %H:%M:%S") for x in results_4],\
                     'x_h': [x[1] for x in results_4]}


    return render_template("index.html", records=json.dumps(marker))
    

@app.route('/_query')
def _query():
    given_date = request.args.get('given_date')
    given_date = datetime.strptime(given_date, "%m/%d/%Y").date()
    
    if given_date == (datetime.utcnow() - timedelta(hours=5)).date():
        marker = "today"
    elif given_date < datetime(2015, 1, 27).date():
        marker = "Please choose a date after Jan 27, 2015."
    elif given_date > (datetime.utcnow() - timedelta(hours=5)).date():
        marker = "Anomaly detection in future is not implemented yet!"
    else:
        print os.getcwd()
        filename = 'anomalies_' + given_date.strftime("%Y-%m-%d") + '.pkl'
        with open('app/static/pickles/' + filename, 'rb') as handle:
            marker = pickle.load(handle)

    return jsonify(result=marker)


@app.route('/slides')
def slides():
    return render_template("slides.html")

@app.route('/about')
def about():
    return render_template("about.html")
