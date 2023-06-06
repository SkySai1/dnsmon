from statistics import mean
from backend.accessdb import AccessDB, getnow, enginer
from threading import Thread
import dns.message
import dns.name
import dns.rdatatype
import dns.query
import dns.rcode
import dns.rrset
import dns.rdtypes
import dns.exception
import logging

class NScheck(Thread):

    def __init__(self, _CONF, ns, group, zones, debug, name = None):
        Thread.__init__(self)
        self.conf = _CONF
        self.value = None
        self.ns = ns
        self.nsname = name
        self.group = group
        self.zones = zones
        self.debug = debug
 
    def run(self):
        self.data = []
        rtime = []
        self.serials = {}
        self.empty = True
        for group in self.zones:
            if group != self.group: continue
            for zone in self.zones[group]:
                self.serials[zone] = {}
                #time.sleep(0.1)
                try:
                    qname = dns.name.from_text(zone)
                    query = dns.message.make_query(qname, dns.rdatatype.SOA)
                    for i in range(5):
                        try:
                            self.answer = dns.query.udp(query, self.ns, self.conf['timeout'])
                            break
                        except dns.exception.Timeout as e:
                            if i >=2: raise e
                    if self.answer.rcode() is not dns.rcode.NOERROR:
                        error = dns.rcode.to_text(self.answer.rcode())
                        self.data.append(f"{zone}: {error}")
                    else:
                        serial = self.answer.answer[0][0].serial
                        self.empty = False
                        self.serials[zone]['serial'] = int(serial)

                    self.serials[zone]['status'] = self.answer.rcode()
                    # Внизу костыль, УБРАТЬ!
                    if self.ns in ['185.247.195.1', '185.247.195.2']: 
                        rtime.append(self.answer.time)
                    # Конец костыля
                except Exception as e:
                    self.serials[zone]['status'] = str(e)
                    self.empty = False
                    self.data.append(f"{zone}: {str(e)}")
                    continue

        # Продолжение костыля. УБРАТЬ!
        if rtime: Nameservers.Kostil(self, self.ns, rtime)

        if self.debug == (2 or 3):
            print(self.nsname, self.ns, self.empty, self.data)


class Nameservers:
    def __init__(self, _CONF):
        self.conf = _CONF
        self.timedelta = _CONF['timedelta']
        self.node = _CONF['node']
    
    # Да-да и это всё тот же костыль!
    def Kostil(self, ns, time):
        stats = [{
            "node": self.conf['node'],
            "ts": getnow(self.conf['timedelta']),
            "server": ns,
            "rtime": mean(time)
        }]
        db = AccessDB(self.conf)
        db.InsertTimeresolve(stats)
    # Конец костыля


    def resolvetime(self, data, db:AccessDB):
        stats = []
        for ns in data:
            stats.append(
                {
                    "node": self.node,
                    "ts": getnow(self.timedelta),
                    "server": ns, 
                    "rtime": mean(data[ns]),
                 }
                 )
        db.InsertTimeresolve(stats)
    
    def parse(self, ns, data, db:AccessDB):
        db.UpdateNS(ns, data)