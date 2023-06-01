#!./mon/bin/python3
import json
import os
import sys
import logging
import dns.name
import dns.message
import dns.rdatatype
import dns.rdataclass

from initconf import getconf
from backend.accessdb import Base, enginer
from backend.recursive import Recursive
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
            #if self.value[0].answer == 0: 
            break
        print(self.value[0].question[0].name, self.value[1:], self.value[0].rcode())

# Make the magic begin
def launch(domains_list):
    stream = []
    for d in domains_list:
        try:
            domain = dns.name.from_text(d)
            t = DomainResolve(domain)
            t.start()
            stream.append(t)
        except: pass
    for t in stream:
        t.join()
        data = t.value
        #print(data[0].question, data[1], data[2])




def get_list(path):
    if os.path.exists(path):
        with open(path, "r") as f:
            list = json.loads(f.read())
            return list

if __name__ == '__main__':
    # -- Get options from config file --
    try:
        _CONF = getconf(sys.argv[1])
    except IndexError:
        print('Specify path to config file')
        sys.exit()

    # -- Try to create tables if it doesnt --
    try: Base.metadata.create_all(enginer(_CONF))
    except: logging.exception('DB INIT:')

    # -- Try to get list of DNS objects to checking it --
    try:
        zones_list = get_list(_CONF['zones'])
        domains_list = get_list(_CONF['domains'])
        ns_list = get_list(_CONF['nameservers'])
    except: 
        logging.exception('GET DATA FROM CONF')
        sys.exit()
    
    launch(domains_list)