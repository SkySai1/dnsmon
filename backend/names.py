import random
import dns.message
import dns.rdatatype
import dns.rcode
import logging
from backend.accessdb import AccessDB, getnow
from backend.recursive import Recursive
from threading import Thread

class NameResolve(Thread):

    def __init__(self, conf, qname, debug, rtype:dns.rdatatype = dns.rdatatype.A):
        Thread.__init__(self)
        self.value = None
        self.conf = conf
        self.qname = qname
        self.debug = debug
        self.rtype = rtype
 
    def run(self):
        query = dns.message.make_query(self.qname, self.rtype)
        R = Recursive(timeout=self.conf['timeout'])
        for i in range(3):
            self.value = R.recursive(query)
            if self.value[0].answer: 
                break
        if self.debug == (1 or 3):
            print(self.value[0].question[0].name, self.value[0].rcode(), self.value[1], self.value[2], self.value[0].time)


class Domains:
    def __init__(self, _CONF) -> None:
        pass

    def parse(self, data:dns.message.Message, auth, db:AccessDB):
        try:
            domain = data.question[0].name.to_text()
            rdata = None
            error = data.rcode()
            if data.answer:
                for rr in data.answer:
                    #if rr.rdtype == dns.rdatatype.A:
                    rdata = ', '.join(str(v) for v in rr)
            else: error = dns.rcode.NXDOMAIN
            db.UpdateDomains(domain, error, auth, rdata)         
        except:
            logging.exception('DOMAIN PARSE:')
            pass
    
    def sync(self, d_list, db:AccessDB):
        try:
            dlist_from_db = db.GetDomain()
            for d in dlist_from_db:
                if not d[0] in d_list:
                    db.RemoveDomain(d[0])
        except Exception as e:
            print(e)

class Zones:
    def __init__(self, conf):
        self.timedelta = conf['timedelta']
        self.node = conf['node']

    def parse(self, data, db:AccessDB):
        zones = {}
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
            db.UpdateZones(zone, status, serial, message)            

    def resolvetime(self, data, db:AccessDB):
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
        db.InsertTimeresolve(stats, False)

    def sync(self, zones, db:AccessDB):
        try:
            zlist_from_db = db.GetZone()
            for group in zones:
                for z in zlist_from_db:
                    if not z[0] in make_fqdn(zones[group]):
                        db.RemoveZone(z[0])
        except Exception as e:
            print(e)

def make_fqdn(dlist):
    new_dlist = []
    for d in dlist:
        if '.' != d[-1]:
            d += '.'
        new_dlist.append(d)
    return new_dlist