#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from influxdb import InfluxDBClient
import requests
from datetime import datetime
from config import LINE_NOTIFY_TOKEN
from pathlib import Path
import os
import urllib.request

RADAR_MAP_URL_BASE = 'http://www.jma.go.jp/jp/radnowc/imgs/radar/211/'
LINE_NOTIFY_API_ENDPOINT = 'https://notify-api.line.me/api/notify'

INFLUXDB_ADDR = '192.168.2.20'
INFLUXDB_PORT = 8086
INFLUXDB_DB = 'sensor'

# NOTE: センサからは最長で 30 分間間隔でデータが登録されるので，
# 余裕を見て過去 1 時間分を取得
INFLUXDB_QUERY = """
SELECT MEAN("touchpad") FROM "sensor.esp32" WHERE ("hostname" = \'ESP32-raindrop\') AND time >= now() - 1h GROUP BY time(2m) FILL(previous) ORDER by time asc
"""

WET_THRESHOLD = 380

NOTIFY_FLAG_FILE = '.notify'
NOTIFY_INTERVAL = 7200

def radar_map_url():
    return RADAR_MAP_URL_BASE + \
        datetime.today().strftime('%Y%m%d%H') \
        + '{:02d}'.format(datetime.today().minute // 5 * 5) \
        + '-00.png'

def notify_flag_file():
    return os.path.join(os.path.dirname(__file__), NOTIFY_FLAG_FILE)

def line_notify(message):
    payload = { 'message': message }
    headers = { 'Authorization': 'Bearer ' + LINE_NOTIFY_TOKEN }
    files = { 'imageFile': urllib.request.urlopen(radar_map_url()) }
    line_notify = requests.post(LINE_NOTIFY_API_ENDPOINT,
                                data=payload, headers=headers, files=files)

    Path(notify_flag_file()).touch()

def check_soil_wet():
    client = InfluxDBClient(host=INFLUXDB_ADDR, port=INFLUXDB_PORT, database=INFLUXDB_DB)
    result = client.query(INFLUXDB_QUERY)
    thresh_below = 0

    status_list = list(map(lambda x: x['mean'], result.get_points()))
    
    thresh_below = 0
    thresh_above = 0

    for val in status_list:
        if (val is None): # NOTE: データが登録されていない期間は None
            continue
        elif (val < WET_THRESHOLD):
            thresh_below += 1
        elif (val > WET_THRESHOLD):
            thresh_above += 1
            thresh_below = 0 # NOTE: reset count

    # DEBUG
    # print('{} (below, above) = ({}, {})'.format(datetime.now(), thresh_below, thresh_above))
    # print(status_list)

    return (thresh_below > 0) and (thresh_above > 0)

def check_already_notified():
    notified_datetime = datetime.fromtimestamp(os.stat(notify_flag_file()).st_mtime)
    elapsed_time = (datetime.now() - notified_datetime).total_seconds()

    return elapsed_time > NOTIFY_INTERVAL


if check_soil_wet() and check_already_notified():
    line_notify('\U00002614' + '️雨が降り始めました！')
