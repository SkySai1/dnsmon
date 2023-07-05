from multiprocessing import Pipe
import random
import dns.message
import dns.rdatatype
import dns.rcode
import logging
from backend.accessdb import getnow, make_fqdn
from backend.recursive import Recursive
from threading import Thread, BoundedSemaphore

class NameResolve(Thread):

    def __init__(self, limit:BoundedSemaphore, _CONF, qname, debug, rtype:dns.rdatatype = dns.rdatatype.A):
        Thread.__init__(self)
        self.limit = limit
        self.timeout = float(_CONF['RECURSION']['timeout'])
        self.maxdepth = int(_CONF['RECURSION']['maxdepth'])
        self.retry = int(_CONF['RECURSION']['retry'])
        self.qname = qname
        self.debug = debug
        self.rtype = rtype
        self.value = None
 
    def run(self):
        self.limit.acquire()
        query = dns.message.make_query(self.qname, self.rtype)
        R = Recursive(timeout=self.timeout, depth=self.maxdepth, retry=self.retry)
        for i in range(3):
            self.value = R.recursive(query)
            if self.value[0].answer: 
                break
        if self.debug in [1,3]:
            print(self.name, self.value[0].question[0].name, self.value[0].rcode(), self.value[1], self.value[2], self.value[0].time)
        self.limit.release()

class Domains:
    def __init__(self, _CONF) -> None:
        pass

    def parse(self, data:dns.message.Message, auth):
        try:
            domain = data.question[0].name.to_text()
            rdata = None
            error = data.rcode()
            if data.answer:
                for rr in data.answer:
                    #if rr.rdtype == dns.rdatatype.A:
                    rdata = ', '.join(str(v) for v in rr)
            else: error = dns.rcode.NXDOMAIN
            return domain, error, auth, rdata    
        except:
            logging.exception('DOMAIN PARSE:')
            pass
    
class Zones:
    def __init__(self, _CONF):
        self.node = _CONF['DATABASE']['node']
        self.timedelta = float(_CONF['DATABASE']['timedelta'])

    def parse(self, data):
        zones = {}
        storage = []
        for ns in data:
            #print(ns)
            for zn in data[ns]:
                #print(zn)
                #print(data[ns][zn])
                if not zn in zones: zones[zn] = {}
                zones[zn][ns] = data[ns][zn]

        for zone in zones:
            serial = 0
            status = 1
            message = []
            for ns in zones[zone]:
                if 'serial' not in zones[zone][ns]: continue
                if zones[zone][ns]['serial'] > serial:
                    serial = zones[zone][ns]['serial']
            for ns in zones[zone]:
                if zones[zone][ns]['status'] is not dns.rcode.NOERROR:
                    status = 0
                    message.append(f"{ns}: {str(zones[zone][ns]['status'])}")
                    continue
                if zones[zone][ns]['serial'] < serial:
                    status = 0
                    message.append(f"{ns}: bad serial - {zones[zone][ns]['serial']} (the right is {serial})")
            message = " & ".join(message)
            try:
                zone = zone.lower().encode().decode('idna')
            except: zone = zone
            storage.append({
                'zone':zone,
                'status': status,
                'serial': serial,
                'message': message
            })
            #db.UpdateZones(zone, status, serial, message)
        return storage            

    def resolvetime(self, data):
        stats = []
        for zn in data:
            stats.append(
                {
                    "node": self.node,
                    "ts": getnow(self.timedelta),
                    "zone": zn, 
                    "rtime": data[zn],
                 }
                 )
        return stats