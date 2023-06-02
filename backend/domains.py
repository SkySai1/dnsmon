import dns.message
import dns.rdatatype
import dns.rcode
import logging
from backend.accessdb import AccessDB
from backend.recursive import Recursive
from threading import Thread

class DomainResolve(Thread):

    def __init__(self, qname, debug):
        Thread.__init__(self)
        self.value = None
        self.qname = qname
        self.debug = debug
 
    def run(self):
        query = dns.message.make_query(self.qname, dns.rdatatype.A)
        R = Recursive()
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
                    if rr.rdtype == dns.rdatatype.A:
                        rdata = ', '.join(str(v) for v in rr)
            db.UpdateDomains(domain, error, auth, rdata)
                        
        except:
            logging.exception('DOMAIN PARSE:')
            pass
    
    def sync(self, d_list, db:AccessDB):
        dlist_from_db = db.GetDomain()
        for d in dlist_from_db:
            if not d[0] in d_list:
                db.RemoveDomain(d[0])

def make_fqdn(dlist):
    new_dlist = []
    for d in dlist:
        if '.' != d[-1]:
            d += '.'
        new_dlist.append(d)
    return new_dlist