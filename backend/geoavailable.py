
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

    def __init__(self, conf, qname, ip):
        Thread.__init__(self)
        self.state = None
        self.value = None
        self.conf = conf
        self.qname = qname
        self.ip = ip
        

 
    def run(self):
        port = 53
        try:
            #result = scan.scan(self.ip, str(port), '-Pn -sV -sU',timeout=5)
            #res = result['scan'][ip]['udp'][53]['state']
            #print(json.dumps(result, indent=4))
            #self.state = result['scan'][self.ip]['udp'][53]['state']
            #if self.state == 'open':
            for i in range(5):
                try:
                    query = dns.message.make_query(self.qname, dns.rdatatype.A)
                    r = dns.query.udp(query, self.ip, 5)
                    if r.rcode() is dns.rcode.NOERROR: 
                        break
                except Exception as e:
                    #print(e)
                    r = None
            if r: self.value = r.rcode()
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
        for country in self.geo:
            j = 0
            for city in self.geo[country]:
                #if j >=2: break
                random.shuffle(self.geo[country][city])
                k = 0
                for server in self.geo[country][city]:
                    if k >=2: break
                    qname = dns.name.from_text(random.choice(domains))
                    T = Scanner(self.conf, qname, server["ip"])
                    T.start()
                    stream.append((T, country, city))
                    k+=1
                j+=1

        for t, country, city in stream:
            t.join()
            if type(t.value) is not NoneType:
                self.db.InsertGeostate(t.ip, t.value)
                #print(f"{country} {city} {t.ip}: {str(t.value)}")
        self.db.RemoveGeo()
        logging.info('Finished geocheck')
        
    
    def clear(self):
        self.db.RemoveGeo()
        

