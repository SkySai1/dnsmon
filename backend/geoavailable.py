
import random
from threading import Thread
import json
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
        self.response = None
        self.state = None
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
                    self.response = dns.query.udp(query, self.ip, 5)
                    if self.response is dns.rcode.NOERROR and self.response.answer: 
                        break
                except Exception as e:
                    pass
        except Exception as e:
            self.response is None


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
        while True:
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
                r = t.response
                if r:
                    if r.answer:
                        r.set_rcode(dns.rcode.NOERROR)
                    elif not r.answer and r.rcode() is dns.rcode.NOERROR:
                        r.set_rcode(dns.rcode.SERVFAIL)
                    self.db.InsertGeostate(t.ip, r.rcode())
            self.db.RemoveGeo()
            self.conf['sleep']
        
    
    def clear(self):
        self.db.RemoveGeo()
        


