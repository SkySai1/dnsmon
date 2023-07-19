
import logging
from multiprocessing import Pipe
import random
from threading import Thread, BoundedSemaphore
import dns.message
import dns.query
import dns.rcode
import dns.rdatatype
import dns.name
from backend.accessdb import AccessDB, getnow


class Scanner(Thread):

    def __init__(self, limit:BoundedSemaphore, timeout, retry, server, domain):
        Thread.__init__(self)
        self.limit = limit
        self.timeout = timeout
        self.retry = retry
        self.server = server
        self.domain = domain
        
    def run(self):
        result = (None, None)
        try:
            qname = dns.name.from_text(self.domain)
            for i in range(self.retry):
                try:
                    query = dns.message.make_query(qname, dns.rdatatype.A)
                    r = dns.query.udp(query, self.server, self.timeout)
                    if r.answer and r.rcode() is dns.rcode.NOERROR: 
                        break
                except Exception as e:
                    r = None
            if r:
                if not r.answer:
                    result = (int(dns.rcode.SERVFAIL), dns.rcode.SERVFAIL.to_text())
                elif r.answer and r.rcode() is dns.rcode.NOERROR:
                    result = (int(r.rcode()), [str(a) for a in r.answer[0]])
                else:
                    result = (int(r.rcode()), r.rcode().to_text())
                             
        except Exception as e:
            pass
        finally:
            self.result = result
            self.limit.release()


class Available:

    def __init__(self, _CONF, limit:BoundedSemaphore, geobase):
        self.limit = limit
        self.timeout = float(_CONF['GEO']['timeout'])
        self.retry = int(_CONF['GEO']['retry'])
        self.maxcities = int(_CONF['GEO']['maxcities'])
        self.maxservers = int(_CONF['GEO']['maxservers'])
        self.node = _CONF['DATABASE']['node']
        self.timedeilta = int(_CONF['DATABASE']['timedelta'])
        self.geodata = Available.get(self, geobase)

    def get(self, data):
        geobase = {}
        for obj in data:
            for row in obj:
                if not row.country in geobase:
                    city = row.city
                    geobase[row.country] = {}
                if not city in geobase[row.country]:
                    geobase[row.country][city] = []
                geobase[row.country][city].append({
                    "ip": row.ip,
                    "latitude": row.latitude,
                    "longitude": row.longitude,
                })
        return geobase
    
    def geocheck(self, domains, child:Pipe=None):
        logging.info("Starting geocheck")
        storage = []
        stream = []
        for country in self.geodata:
            j = 0
            for city in self.geodata[country]:
                if j>=self.maxcities: break
                random.shuffle(self.geodata[country][city])
                k = 0
                for server in self.geodata[country][city]:
                    if k>=self.maxservers: break
                    domain = random.choice(domains)
                    self.limit.acquire()            
                    T = Scanner(self.limit, self.timeout, self.retry, server['ip'], domain)
                    T.start()
                    stream.append((T, server, domain))
                    k += 1
                j+=1
        for s in stream:
            s[0].join()
            if s[0].result[0] is not None:
                storage.append({
                    'node': self.node,
                    'ip': s[1]['ip'],
                    'ts': getnow(self.timedeilta),
                    'domain': s[2],
                    'state': s[0].result[0],
                    'result': ",".join(s[0].result[1])
                })
            #print(s[0].result, s[1]['latitude'], s[1]['longitude'])
        child.send(storage)
        logging.info('Finished geocheck')
