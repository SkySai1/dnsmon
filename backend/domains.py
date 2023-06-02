import dns.message
import dns.rdatatype
import dns.rcode
import logging

from backend.accessdb import AccessDB

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