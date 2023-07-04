from multiprocessing import Pipe
from statistics import mean
from backend.accessdb import AccessDB, getnow, enginer
from backend.names import make_fqdn
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

    def __init__(self, name, _CONF, ns, group, zones, debug, nsname = None):
        Thread.__init__(self)
        self.name = name
        self.retry = int(_CONF['RESOLVE']['retry'])
        self.timeout = float(_CONF['RESOLVE']['timeout'])
        self.value = None
        self.ns = ns
        self.nsname = nsname
        self.group = group
        self.zones = zones
        self.debug = debug
 
    def run(self):
        self.data = []
        rtime = []
        self.serials = {}
        self.empty = True
        self.state = False
        for group in self.zones:
            if group != self.group: continue
            for zone in make_fqdn(self.zones[group]):
                self.serials[zone] = {}
                #time.sleep(0.1)
                try:
                    qname = dns.name.from_text(zone)
                    query = dns.message.make_query(qname, dns.rdatatype.SOA)
                    for i in range(self.retry):
                        try:
                            self.answer = dns.query.udp(query, self.ns, self.timeout)
                            self.state = True
                            break
                        except: pass
                    if self.answer.rcode() is not dns.rcode.NOERROR:
                        error = dns.rcode.to_text(self.answer.rcode())
                        self.data.append(f"{zone}: {error}")
                    else:
                        serial = self.answer.answer[0][0].serial
                        self.empty = False
                        self.serials[zone]['serial'] = int(serial)

                    self.serials[zone]['status'] = self.answer.rcode()
                except Exception as e:
                    self.serials[zone]['status'] = str(e)
                    self.empty = False
                    #self.data.append(f"{zone}: {str(e)}")
                    continue
        if self.state is False:
            self.data.append(f"this ns ({self.ns}) is unvailable")

        if self.debug == (2 or 3):
            print(self.nsname, self.ns, self.empty, self.data)


class Nameservers:
    def __init__(self, _CONF):
        self.timedelta = int(_CONF['DATABASE']['timedelta'])
        self.node = _CONF['DATABASE']['node']
    
    def resolvetime(self, data):
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
        return stats
    
    def parse(self, ns, data, db:AccessDB):
        db.UpdateNS(ns, data)

    def sync(self, nslist, db:AccessDB, child:Pipe=None):
        try:
            nslist_from_db = db.GetNS()
            nsnames = []
            for addr in nslist:
                nsnames.append(nslist[addr][0])

            for ns in nslist_from_db:
                if not ns[0] in nsnames:
                    db.RemoveNS(ns[0])
        except Exception as e:
            print(e)