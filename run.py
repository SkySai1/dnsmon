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
from backend.healthcheck import HealthCheck
from initconf import getconf
from backend.namservers import Nameservers, NScheck
from backend.accessdb import Base, enginer, AccessDB
from backend.names import Domains, NameResolve, Zones, make_fqdn
from backend.geoavailable import Available
from threading import BoundedSemaphore
import random

# Modification of classic Threading

# --Make the magic begin
def launch_domain_check(domains_list, ns_list, _CONF, child:Pipe):
    logging.info("Started domains check")
    # -- Make resolve in another thread for each domain --
    storage = {}
    stream = []
    for d in domains_list:
        try:
            domain = dns.name.from_text(d)
            _MAXTHREADS.acquire()
            t = NameResolve(_MAXTHREADS, _CONF, domain, _DEBUG)
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
    logging.info("End domains check")
    #for ns in ns_stats: print(ns)

# --NameServer checking
def launch_ns_and_zones_check(nslist, zones, _CONF, child:Pipe=None):
    logging.info("Started NS and zones check")
    stream = []
    storage = {}
    Z = Zones(_CONF)
    for ns in nslist:
        name = random.randint(1, int(_CONF['GENERAL']['maxthreads']))
        group = nslist[ns][1]
        nsname = nslist[ns][0]
        _MAXTHREADS.acquire()
        t = NScheck(_MAXTHREADS, _CONF, ns, group, zones, _DEBUG, nsname)
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
    logging.info("End NS and zones check")

# --Zones Trace Resolve
def launch_zones_resolve(zones, _CONF, child:Pipe):
    logging.info("Started zone resolving")
    # -- Make resolve in another thread for each zone --
    stream = []
    storage = {}
    storage['FULLRESOLVE'] = []
    for group in zones:
        for zone in zones[group]:
            try:
                zone = dns.name.from_text(zone)
                _MAXTHREADS.acquire()
                t = NameResolve(_MAXTHREADS, _CONF, zone, _DEBUG, dns.rdatatype.SOA)
                t.start()
                stream.append(t)
            except: pass

    # -- Collect data from each thread and do things --
    ns_stats = {}
    Z = Zones(_CONF)
    zn_stats = {}
    for t in stream:
        t.join()
        data, _, rt = t.value
        zn = data.question[0].name.to_text()
        try: zn = zn.lower().encode().decode('idna')
        except: pass
        if not zn in ns_stats: zn_stats[zn] = []
        if data.rcode() is not dns.rcode.NOERROR: rt = 0
        zn_stats[zn] = rt
    if zn_stats: 
        storage['FULLRESOLVE'] = Z.resolvetime(zn_stats)
    child.send(storage)
    logging.info("End zone resolving")

def launch_healthcheck(hc_list, _CONF, child:Pipe()):
    logging.info("Started healthchecking")
    result=[]
    stream = []
    for domain in hc_list:
        _MAXTHREADS.acquire()
        T = HealthCheck(_MAXTHREADS, _CONF, domain, hc_list[domain])
        T.start()
        stream.append(T)
    for t in stream:
        t.join()
        result.append(t.result)
    child.send(result)
    logging.info("End healthchecking")

def get_list(path):
    with open(path, "r") as f:
        list = json.loads(f.read())
        return list

# Мультипроцессинг:
def PipeParallel(data):
    try:
        proc = []
        storage = {}
        channel = []
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
                channel.append((fn, parent))
        for p in channel:
            storage[p[0].__name__] = p[1].recv()
        return storage
    except Exception:
        logging.exception('PROCCESSING')

def handler(event=None, context=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # -- Get options from config file --
    logging.info("DNSCHECKER IS RAN!!!")
    try:
        path = os.path.abspath(sys.argv[1])
        thisdir = os.path.dirname(os.path.abspath(__file__))
        _CONF = getconf(path)
        global _DEBUG
        _DEBUG = int(_CONF['GENERAL']['debug'])
        global _MAXTHREADS
        _MAXTHREADS = BoundedSemaphore(int(_CONF['GENERAL']['maxthreads']))
    except IndexError:
        print('Specify path to config file')
        sys.exit()

    # -- Try to get list of DNS objects to checking it --
    try:
        dpath = re.sub(r'^\.',thisdir,_CONF['FILES']['domains'])
        nspath = re.sub(r'^\.',thisdir,_CONF['FILES']['nameservers'])
        zonepath = re.sub(r'^\.',thisdir,_CONF['FILES']['zones'])
        healthpath = re.sub(r'^\.',thisdir,_CONF['FILES']['healthcheck'])
        domains_list = make_fqdn(get_list(dpath))
        ns_list = get_list(nspath)
        zones_list = get_list(zonepath)
        hc_list = get_list(healthpath)
    except: 
        logging.exception('GET DATA FROM CONF')
        sys.exit()

    # -- Try to create tables if it doesnt --
    try: 
        Base.metadata.create_all(enginer(_CONF))
        init = AccessDB(_CONF)
        geobase = init.start(domains_list, zones_list, ns_list)
    except: 
        logging.exception('DB INIT:')
        sys.exit()


    geo = Available(_CONF, _MAXTHREADS, geobase)
    processes = [
        #{launch_domain_check: [domains_list, ns_list, _CONF]},
        #{launch_ns_and_zones_check: [ns_list, zones_list, _CONF]},
        #{launch_zones_resolve: [zones_list, _CONF]},
        {launch_healthcheck: [hc_list, _CONF]}
        #{geo.geocheck: [domains_list]}
    ]
    try:
        STORAGE = PipeParallel(processes)
        print(json.dumps(STORAGE, indent=4))
        DB = AccessDB(_CONF, STORAGE)
        DB.parse()
    except KeyboardInterrupt:
        pass
    finally:
        logging.info("DNSCHECKER IS END.")

if __name__ == "__main__":
    handler() # <- for manual start
