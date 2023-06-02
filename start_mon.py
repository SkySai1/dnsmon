#!./mon/bin/python3
import json
import os
import sys
import logging
import time
import dns.name
import dns.message
import dns.rdatatype
import dns.rdataclass
import dns.rcode
from multiprocessing import Process
from initconf import getconf
from backend.namservers import Nameservers
from backend.accessdb import Base, enginer, AccessDB
from backend.recursive import Recursive
from backend.domains import Domains, make_fqdn
from threading import Thread


# Modification of classic Threading
class DomainResolve(Thread):

    def __init__(self, qname):
        Thread.__init__(self)
        self.value = None
        self.qname = qname
 
    def run(self):
        query = dns.message.make_query(self.qname, dns.rdatatype.A)
        R = Recursive()
        for i in range(3):
            self.value = R.recursive(query)
            if self.value[0].answer: 
                break
        if _DEBUG > 0:
            print(self.value[0].answer, self.value[1:], self.value[0].rcode())

# Make the magic begin
def launch(domains_list):
    # -- Make resolve in another thread for each domain --
    stream = []
    for d in domains_list:
        try:
            domain = dns.name.from_text(d)
            t = DomainResolve(domain)
            t.start()
            stream.append(t)
        except: pass


    # -- Collect data from each thread and do things --
    ns_stats = {}
    D = Domains(_CONF)
    NS = Nameservers(_CONF)
    db = AccessDB(_CONF)
    for t in stream:
        t.join()
        data, ip, rt = t.value
        if ip in ns_list: 
            auth = ns_list[ip][1]
            ns = ns_list[ip][0]
        else: 
            auth = ns = None
        D.parse(data, auth, db) # <- preparing resolved data to load into DB
        if data.rcode() == dns.rcode.NOERROR and ns:
            if not ns in ns_stats: ns_stats[ns] = []
            ns_stats[ns].append((rt, data.time))
    if ns_stats: NS.resolvetime(ns_stats, db)
    #for ns in ns_stats: print(ns)




def get_list(path):
    with open(path, "r") as f:
        list = json.loads(f.read())
        return list

# Мультипроцессинг:
def Parallel(data):
    proc = []
    for pos in data:
        for fn in pos:
            if type(pos[fn]) is dict:
                p = Process(target=fn, kwargs=pos[fn])
                p.start()
                proc.append(p)
            else:
                p = Process(target=fn, args=pos[fn])
                p.start()
                proc.append(p)
    for p in proc:
        p.join()

if __name__ == '__main__':
    # -- Get options from config file --
    try:
        _CONF = getconf(sys.argv[1])
        _DEBUG = _CONF['debug']
    except IndexError:
        print('Specify path to config file')
        sys.exit()

    # -- Try to create tables if it doesnt --
    try: 
        Base.metadata.create_all(enginer(_CONF))
        init = AccessDB(_CONF)
        init.NewNode()
    except: 
        logging.exception('DB INIT:')
        sys.exit()

    # -- Try to get list of DNS objects to checking it --
    try:
        zones_list = get_list(_CONF['zones'])
        domains_list = make_fqdn(get_list(_CONF['domains']))
        ns_list = get_list(_CONF['nameservers'])
    except: 
        logging.exception('GET DATA FROM CONF')
        sys.exit()
    


    sD = Domains(_CONF)
    sDdb = AccessDB(_CONF)
    processes = [
        {launch: [domains_list]},
        {sD.sync: [domains_list, sDdb]}
    ]
    try:
        while True:
            Parallel(processes)
            time.sleep(_CONF['refresh'])
    except KeyboardInterrupt:
        pass