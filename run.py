#!/etc/dev/dnschecker/mon/bin/python3
import json
import os
import re
import sys
import logging
import time
import dns.name
import dns.message
import dns.rdatatype
import dns.rdataclass
import dns.rcode
from multiprocessing import Process, Pipe
from initconf import getconf
from backend.namservers import Nameservers, NScheck
from backend.accessdb import Base, enginer, AccessDB
from backend.names import Domains, NameResolve, Zones, make_fqdn
from backend.geoavailable import Available
from threading import Thread
import random

# Modification of classic Threading

# --Make the magic begin
def launch_domain_check(domains_list, ns_list, _CONF, child:Pipe):
    logging.info("Started domains check")
    # -- Make resolve in another thread for each domain --
    storage = {}
    stream = []
    for d in domains_list:
        name = random.randint(1, int(_CONF['GENERAL']['maxthreads']))
        try:
            domain = dns.name.from_text(d)
            t = NameResolve(name, _CONF, domain, _DEBUG)
            t.start()
            stream.append(t)
        except Exception: 
            logging.exception('NAME RESOLVING')
            pass

    # -- Collect data from each thread and do things --
    ns_stats = {}
    D = Domains(_CONF)
    NS = Nameservers(_CONF)
    storage['DOMAINS'] = []
    for t in stream:
        t.join()
        data, ip, rt = t.value
        if ip in ns_list: 
            auth = ns_list[ip][1]
            ns = ns_list[ip][0]
        else: 
            auth = ns = None
        result = D.parse(data, auth)
        storage['DOMAINS'].append({ # <- preparing resolved data to load into DB
                'domain': result[0],
                'error': result[1],
                'auth': result[2],
                'rdata': result[3]
            }) 
        if data.rcode() == dns.rcode.NOERROR and ns:
            if not ns in ns_stats: ns_stats[ns] = []
            ns_stats[ns].append(data.time)
    if ns_stats: 
        storage['SHORTRESOLVE'] = NS.resolvetime(ns_stats)
    child.send(storage)
    logging.info("Ended domains check")
    #for ns in ns_stats: print(ns)

# --NameServer checking
def launch_ns_and_zones_check(nslist, zones, _CONF, child:Pipe=None):
    logging.info("Started NS and zones check")
    stream = []
    storage = {}
    NS = Nameservers(_CONF)
    Z = Zones(_CONF)
    for ns in nslist:
        name = random.randint(1, int(_CONF['GENERAL']['maxthreads']))
        group = nslist[ns][1]
        nsname = nslist[ns][0]
        t = NScheck(name, _CONF, ns, group, zones, _DEBUG, nsname)
        t.start()
        stream.append(t)
    stats = {}
    storage['NS'] = []
    for t in stream:
        t.join()
        ns = t.ns
        if ns in nslist: ns = nslist[ns][0]
        if t.empty is False:
            storage['NS'].append({
                'ns': ns,
                'message':t.data
            })
            #NS.parse(ns, t.data, db)
            stats[ns] = t.serials
    storage['ZONES'] = Z.parse(stats)
    child.send(storage)
    logging.info("Ended NS and zones check")

# --Zones Trace Resolve
def launch_zones_resolve(zones, _CONF, _DEBUG = None, child:Pipe=None):
    logging.info("Started zone resolving")
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
    logging.info("Ended zone resolving")

def get_list(path):
    with open(path, "r") as f:
        list = json.loads(f.read())
        return list

# Мультипроцессинг:
def Parallel(data):
    proc = []
    data = []
    parent, child = Pipe()
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

def PipeParallel(data):
    proc = []
    storage = {}
    for pos in data:
        for fn in pos:
            parent, child = Pipe()
            if type(pos[fn]) is dict:
                pos[fn]['child'] = child
                p = Process(target=fn, kwargs=pos[fn])
                p.start()
                proc.append(p)
            else:
                pos[fn].append(child)
                p = Process(target=fn, args=pos[fn])
                p.start()
                proc.append(p)
            storage[fn.__name__] = parent.recv()
    return storage

def handler(event=None, context=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # -- Get options from config file --
    logging.info("dnschecker is run!")
    try:
        thisdir = os.path.dirname(os.path.abspath(__file__))
        _CONF = getconf(thisdir+'/config.conf')
        global _DEBUG
        _DEBUG = int(_CONF['GENERAL']['debug'])
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
        dpath = re.sub(r'^\.',thisdir,_CONF['FILES']['domains'])
        nspath = re.sub(r'^\.',thisdir,_CONF['FILES']['nameservers'])
        zonepath = re.sub(r'^\.',thisdir,_CONF['FILES']['zones'])
        domains_list = make_fqdn(get_list(dpath))
        ns_list = get_list(nspath)
        zones = get_list(zonepath)
    except: 
        logging.exception('GET DATA FROM CONF')
        sys.exit()
    


    domain_service = Domains(_CONF)
    zone_service = Zones(_CONF)
    ns_service = Nameservers(_CONF)
    #geo = Available(_CONF, geoDB)
    processes = [
        {launch_domain_check: [domains_list, ns_list, _CONF]},
        {launch_ns_and_zones_check: [ns_list, zones, _CONF]}
        #{launch_zones_resolve: [zones, _CONF]},
        #{domain_service.sync: [domains_list, domainDB]},
        #{zone_service.sync: [zones, zoneDB]},
        #{ns_service.sync: [ns_list, nsDB]},
        #{geo.start: [domains_list]}
    ]
    try:
        STORAGE = PipeParallel(processes)
        #print(STORAGE['launch_ns_and_zones_check']['ZONES'][13])
        DB = AccessDB(_CONF, STORAGE)
        DB.parse()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    handler() # <- for manual start