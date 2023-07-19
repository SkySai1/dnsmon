from multiprocessing import Pipe
from statistics import mean
import uuid
from backend.accessdb import getnow, make_fqdn
from threading import Thread, BoundedSemaphore
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

    def __init__(self, limit:BoundedSemaphore, _CONF, ns, group, zones, debug, nsname = None):
        Thread.__init__(self)
        self.limit = limit
        self.retry = int(_CONF['RESOLVE']['retry'])
        self.timeout = float(_CONF['RESOLVE']['timeout'])
        self.value = None
        self.ns = ns
        self.nsname = nsname
        self.group = group
        self.zones = zones
        self.debug = debug
 
    def run(self):
        self.value = None
        rtime = []
        self.serials = {}
        self.state = False
        self.bad = None
        for group in self.zones:
            if group != self.group: continue
            for zone in make_fqdn(self.zones[group]):
                self.serials[zone] = {}
                error = None
                try:
                    qname = dns.name.from_text(zone)
                    query = dns.message.make_query(qname, dns.rdatatype.SOA)
                    for i in range(self.retry):
                        try:
                            self.answer = dns.query.udp(query, self.ns, self.timeout)
                            self.state = True
                        except Exception as e: 
                            self.bad = e
                            pass
                    if hasattr(self, 'answer'):
                        if self.answer.rcode() is not dns.rcode.NOERROR:
                            error = dns.rcode.to_text(self.answer.rcode())
                        serial = self.answer.answer[0][0].serial
                        self.serials[zone]['status'] = self.answer.rcode()
                        self.serials[zone]['serial'] = int(serial)
                        #self.data.append(f"{zone}: {error}")

                    else:
                        self.serials[zone]['status'] = str(self.bad)
                        self.serials[zone]['serial'] = 0

                except Exception as e:
                    logging.exception('NScheck')
                    self.serials[zone]['status'] = str(e)
                    self.data.append(f"{zone}: {str(e)}")
                    continue

            if self.state is False:
                print(self.ns)
                self.value = f"({self.ns}) is unvailable"

        if self.debug == (2 or 3):
            print(self.nsname, self.ns, self.value)
        self.limit.release()


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