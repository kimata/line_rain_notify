#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from influxdb import InfluxDBClient
import requests
import time
from datetime import datetime
from config import LINE_NOTIFY_TOKEN
from pathlib import Path
import os
import urllib.request

import traceback

import logging
import logging.handlers
import gzip

RADAR_MAP_URL_BASE = 'http://www.jma.go.jp/jp/radnowc/imgs/radar/211/'
RADAR_MAP_URL_BASE2 = 'https://static.tenki.jp/static-images/radar/%Y/%m/%d/%H/{:02d}/00/pref-26-large.jpg'

LINE_NOTIFY_API_ENDPOINT = 'https://notify-api.line.me/api/notify'

INFLUXDB_ADDR = '192.168.2.20'
INFLUXDB_PORT = 8086
INFLUXDB_DB = 'sensor'

# NOTE: センサからは最長で 30 分間間隔でデータが登録されるので，
# 余裕を見て過去 1 時間分を取得
INFLUXDB_QUERY = """
SELECT MEAN("touchpad") FROM "sensor.esp32" WHERE ("hostname" = \'ESP32-raindrop\') AND time >= now() - 1h GROUP BY time(2m) FILL(previous) ORDER by time asc
"""

WET_ON_THRESHOLD = 370
WET_OFF_THRESHOLD = 380

NOTIFY_FLAG_FILE = '.notify'
NOTIFY_INTERVAL = 7200

def radar_map_url():
#     return RADAR_MAP_URL_BASE + \
#         datetime.today().strftime('%Y%m%d%H') \
#         + '{:02d}'.format(datetime.today().minute // 5 * 5) \
#         + '-00.png'

    url = datetime.today().strftime(RADAR_MAP_URL_BASE2).format(datetime.today().minute // 5 * 5)
    logger.info('radar_map_url: {}'.format(url))

    return url


def notify_flag_file():
    return os.path.join(os.path.dirname(__file__), NOTIFY_FLAG_FILE)

def line_notify(message, image):
    payload = { 'message': message }
    headers = { 'Authorization': 'Bearer ' + LINE_NOTIFY_TOKEN }
    files = { }

    try:
        files = { 'imageFile': image }
    except:
        pass

    res = requests.post(LINE_NOTIFY_API_ENDPOINT,
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
        elif (val < WET_ON_THRESHOLD):
            thresh_below += 1
        elif (val > WET_OFF_THRESHOLD):
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


class GZipRotator:
    def __call__(self, source, dest):
        os.rename(source, dest)
        f_in = open(dest, 'rb')
        f_out = gzip.open("%s.gz" % dest, 'wb')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()
        os.remove(dest)



logger = logging.getLogger()
log_handler = logging.handlers.RotatingFileHandler(
    'line_rain_notify.log',
    encoding='utf8', maxBytes=10*1024*1024, backupCount=10,
)
log_handler.formatter = logging.Formatter(
    fmt='%(asctime)s %(levelname)s %(name)s :%(message)s',
    datefmt='%Y/%m/%d %H:%M:%S %Z'
)
log_handler.formatter.converter = time.gmtime
log_handler.rotator = GZipRotator()

logger.addHandler(log_handler)
logger.setLevel(level=logging.INFO)


try:
    if check_soil_wet() and check_already_notified():
        line_notify('\U00002614' + '️雨が降り始めました！',
                    urllib.request.urlopen(radar_map_url()))
except:
    logger.error(traceback.format_exc())
