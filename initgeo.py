#!./mon/bin/python3
import json
import logging
import random
import sys
from threading import Thread
import urllib.request
import requests
import re
from backend.accessdb import AccessDB, Base, enginer
from initconf import getconf
from run import get_list

class Scrapper:
    def __init__(self, conf, db:AccessDB):
        self.conf = conf
        self.db = db

    def getnslist(self, url):
        response = urllib.request.urlopen(url)
        data = json.loads(response.read())
        return data

    def scrap(self, url):
        data = Scrapper.getnslist(self, url)
        random.shuffle(data)
        i = 0
        for one in data:
            if i >= self.conf['count']: break
            ip = one['ip']
            try:
                location, ip = Scrapper.geocheck(self, ip)
                country = location["country"]
                city = location["city"]
                coordinates = location["coordinates"]
                if coordinates and re.match('^[0-9]+.[0-9]+.[0-9]+.[0-9]+$', ip):
                    self.db.InsertGeobase(ip, coordinates[0], coordinates[1], city, country)
                    i+=1
            except Exception as e:
                pass

    def geocheck(self, ip):
        token = self.conf['token']
        url = f'http://ipinfo.io/{ip}?token={token}'
        response = requests.get(url)
        data = response.json()
        #print(json.dumps(data, indent=4))
        location = {
            "coordinates": data["loc"].split(','),
            "city": data["city"],
            "country": data["country"]
        }
        return location, data["ip"]


if __name__ == "__main__":
    try:
        _CONF = getconf(sys.argv[1])
        _DEBUG = _CONF['debug']
    except IndexError:
        print('Specify path to config file')
        sys.exit()

    try: 
        Base.metadata.create_all(enginer(_CONF))
    except: 
        logging.exception('DB INIT:')
        sys.exit()

    try:
        urllist = get_list(_CONF['publicns'])
    except: 
        logging.exception('GET DATA FROM CONF')
        sys.exit()
        
    db = AccessDB(_CONF)
    S = Scrapper(_CONF, db)
    for url in urllist:
        print(url)
        #S.scrap(url)