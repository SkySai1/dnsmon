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

    def __init__(self, _CONF, ns, group, zones, debug):
        Thread.__init__(self)
        self.conf = _CONF
        self.value = None
        self.ns = ns
        self.group = group
        self.zones = zones
        self.debug = debug
 
    def run(self):
        self.data = []
        rtime = []
        self.empty = True
        for group in self.zones:
            if group != self.group: continue
            for zone in self.zones[group]:
                #time.sleep(0.1)
                try:
                    qname = dns.name.from_text(zone)
                    query = dns.message.make_query(qname, dns.rdatatype.SOA)
                    for i in range(3):
                        try:
                            self.answer = dns.query.udp(query, self.ns, timeout=5)
                            break
                        except Exception as e:
                            i+=1
                            if i >=3: raise e
                    if self.answer.rcode() is not dns.rcode.NOERROR:
                        error = dns.rcode.to_text(self.answer.rcode())
                        self.data.append(f"{zone}: {error}")

                    # Внизу костыль, УБРАТЬ!
                    if self.ns in ['185.247.195.1', '185.247.195.2']: 
                        rtime.append(self.answer.time)

                    else: self.empty = False
                except dns.exception.Timeout as timeout:
                    self.empty = False
                    self.data.append(f"{zone}: {str(timeout)}")
                    continue
                except Exception as e:
                    self.empty = False
                    continue

        # Продолжение костыля. УБРАТЬ!
        if rtime: Nameservers.Kostil(self, self.ns, rtime)

        if self.debug == (2 or 3):
            print(self.ns, self.empty, self.data)


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
            "rtime_short": mean(time)
        }]
        db = AccessDB(self.conf)
        db.InsertTimeresolve(stats)


    def resolvetime(self, data, db:AccessDB):
        stats = []
        for ns in data:
            full = []
            short = []
            for time in data[ns]:
                full.append(time[0])
                short.append(time[1])
            stats.append(
                {
                    "node": self.node,
                    "ts": getnow(self.timedelta),
                    "server": ns, 
                    "rtime": mean(full),
                    "rtime_short": mean(short)
                 }
                 )
        db.InsertTimeresolve(stats)
    
    def parse(self, ns, data, db:AccessDB):
        db.UpdateNS(ns, data)