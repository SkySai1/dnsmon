#!/etc/dev/dnschecker/mon/bin/python3
import json
import logging
import os
import random
import sys
from threading import Thread
import urllib.request
import requests
import re
from backend.accessdb import AccessDB, Base, enginer
from backend.geoavailable import Available
from initconf import getconf
from run import get_list

count = 0

class Scrapper:
    def __init__(self, _CONF, geobase):
        self.count = int(_CONF['GEO']['initcount'])
        self.geobase = geobase

    def getnslist(self, url):
        response = urllib.request.urlopen(url)
        data = json.loads(response.read())
        return data

    def scrap(self, url):
        geobase = []
        data = Scrapper.getnslist(self, url)
        random.shuffle(data)
        i = 0
        for one in data:
            if i >= self.count: break
            ip = one['ip']
            if ip in self.geobase:
                continue
            try:
                location, ip = Scrapper.geocheck(self, ip)
                if location and ip:
                    country = location["country"]
                    city = location["city"]
                    coordinates = location["coordinates"]
                    if coordinates and re.match('^[0-9]+.[0-9]+.[0-9]+.[0-9]+$', ip):
                        geobase.append({
                            'ip': ip,
                            'latitude': coordinates[0],
                            'longitude': coordinates[1],
                            'country': country,
                            'city': city
                        })
                        print('NEW:', country, city, ip)
                        i+=1
            except Exception as e:
                logging.exception('GEO SCRAP')
        return geobase

    def geocheck(self, ip):
        token = _token
        url = f'http://ipinfo.io/{ip}?token={token}'
        response = requests.get(url)
        data = response.json()
        #print(json.dumps(data, indent=4))
        if "loc" in data and "city" in data and "country" in data:
            location = {
                "coordinates": data["loc"].split(','),
                "city": data["city"],
                "country": data["country"]
            }
            return location, data["ip"]
        return None, None


if __name__ == "__main__":
    try:
        thisdir = os.path.dirname(os.path.abspath(__file__))
        _CONF = getconf(thisdir+'/config.conf')
        _DEBUG = _CONF['GENERAL']['debug']
        _token = sys.argv[1]
    except IndexError:
        print('Specify ipinfo.io API token')
        sys.exit()

    try: 
        Base.metadata.create_all(enginer(_CONF))
        DB = AccessDB(_CONF)
        geobase = Available.get(None,DB.start(onlygeo=True))
        geobase = [server['ip'] for line in geobase for city in geobase[line] for server in geobase[line][city]]
        geobase = set(geobase)
    except: 
        logging.exception('DB INIT:')
        sys.exit()

    try:
        urllist = get_list(_CONF['FILES']['publicns'])
    except: 
        logging.exception('GET DATA FROM CONF')
        sys.exit()

    S = Scrapper(_CONF, geobase)
    geostorage = {}
    geostorage['initgeo'] = []
    for url in urllist:
        geostorage['initgeo'].append(S.scrap(url))
        pass
    geostorage['initgeo'] = [data for data in geostorage['initgeo'] for data in data]
    #print(geostorage['initgeo'])
    DB.parse(geostorage)