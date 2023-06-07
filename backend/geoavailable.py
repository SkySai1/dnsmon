
import logging
import random
from threading import Thread
from types import NoneType
import dns.message
import dns.query
import dns.rcode
import dns.rdatatype
import dns.name
from backend.accessdb import AccessDB, getnow


class Scanner(Thread):

    def __init__(self, conf, data, domains, db:AccessDB):
        Thread.__init__(self)
        self.state = None
        self.value = None
        self.conf = conf
        self.data = data
        self.domains = domains
        self.db = db
        

 
    def run(self):
        port = 53
        try:
            random.shuffle(self.data)
            k = 0
            for server in self.data:
                if k >=2: break
                qname = dns.name.from_text(random.choice(self.domains))
                for i in range(2):
                    try:
                        query = dns.message.make_query(qname, dns.rdatatype.A)
                        r = dns.query.udp(query, server["ip"], 5)
                        if r.rcode() is dns.rcode.NOERROR: 
                            break
                    except Exception as e:
                        r = None
                k+=1
                if r: 
                    self.db.InsertGeostate(server["ip"], r.rcode())
        except Exception as e:
            self.state = 'closed'


class Available:

    def __init__(self, conf, db:AccessDB):
        self.conf = conf
        self.db = db
        self.geo = Available.get(self, self.db)

    def get(self, db:AccessDB):
        geobase = {}
        data = db.GetGeo()
        for obj in data:
            for row in obj:
                if not row.country in geobase:
                    city = row.city
                    geobase[row.country] = {}
                if not city in geobase[row.country]:
                    geobase[row.country][city] = []

                geobase[row.country][city].append({
                    "ip": row.ip,
                    "ip": row.ip,
                    "latitude": row.latitude,
                    "longitude": row.longitude,
                })
        return geobase
    
    def start(self, domains):
        logging.info("Starting geocheck")
        stream = []
        db = AccessDB(self.conf)
        for country in self.geo:
            j = 0
            for city in self.geo[country]:
                T = Scanner(self.conf, self.geo[country][city], domains, db)
                T.start()
                stream.append(T)
        for t in stream:
            t.join()
        logging.info('Finished geocheck')
        
    
    def clear(self):
        self.db.RemoveGeo()
        


