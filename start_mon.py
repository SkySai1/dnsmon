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
from backend.namservers import Nameservers, NScheck
from backend.accessdb import Base, enginer, AccessDB
from backend.names import Domains, NameResolve, Zones, make_fqdn
from threading import Thread


# Modification of classic Threading

# --Make the magic begin
def launch_domain_check(domains_list):
    # -- Make resolve in another thread for each domain --
    stream = []
    for d in domains_list:
        try:
            domain = dns.name.from_text(d)
            t = NameResolve(_CONF, domain, _DEBUG)
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
            ns_stats[ns].append(data.time)
    if ns_stats: NS.resolvetime(ns_stats, db)
    #for ns in ns_stats: print(ns)

# --NameServer checking
def launch_ns_check(nslist, zones):
    stream = []
    db = AccessDB(_CONF)
    NS = Nameservers(_CONF)
    for ns in nslist:
        group = nslist[ns][1]
        name = nslist[ns][0]
        t = NScheck(_CONF, ns, group, zones, _DEBUG, name)
        t.start()
        stream.append(t)
    for t in stream:
        t.join()
        ns = t.ns
        if ns in nslist: ns = nslist[ns][0]
        if t.empty is False:
            NS.parse(ns, t.data, db)

# --Zones Trace Resolve
def launch_zones_check(zones):
    # -- Make resolve in another thread for each zone --
    stream = []
    for group in zones:
        for zone in zones[group]:
            try:
                zone = dns.name.from_text(zone)
                t = NameResolve(_CONF, zone, _DEBUG, dns.rdatatype.SOA)
                t.start()
                stream.append(t)
            except: pass

    # -- Collect data from each thread and do things --
    ns_stats = {}
    Z = Zones(_CONF)
    db = AccessDB(_CONF)
    zn_stats = {}
    for t in stream:
        t.join()
        data, _, rt = t.value
        zn = data.question[0].name.to_text()
        if not zn in ns_stats: zn_stats[zn] = []
        if data.rcode() is not dns.rcode.NOERROR: rt = 0
        zn_stats[zn] = rt
    if zn_stats: Z.resolvetime(zn_stats, db)

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
        domains_list = make_fqdn(get_list(_CONF['domains']))
        ns_list = get_list(_CONF['nameservers'])
        zones = get_list(_CONF['zones'])
    except: 
        logging.exception('GET DATA FROM CONF')
        sys.exit()
    


    domain_service = Domains(_CONF)
    domainDB = AccessDB(_CONF)
    processes = [
        #{launch_domain_check: [domains_list]},
        #{launch_ns_check: [ns_list, zones]},
        {launch_zones_check: [zones]},
        {domain_service.sync: [domains_list, domainDB]}
    ]
    try:
        while True:
            Parallel(processes)
            time.sleep(_CONF['refresh'])
    except KeyboardInterrupt:
        pass