import ipaddress
import random
import socket
import threading
import time
import dns.message
import dns.rrset
import dns.query
import dns.exception
import dns.rdatatype
import dns.rdataclass
import dns.rcode
import dns.name
import dns.flags
import logging

_ROOT = [
    "198.41.0.4",           #a.root-servers.net.
    "199.9.14.201",         #b.root-servers.net.
    "192.33.4.12",          #c.root-servers.net.
    "199.7.91.13",          #d.root-servers.net.
    "192.203.230.10",       #e.root-servers.net.
    "192.5.5.241",          #f.root-servers.net.
    "192.112.36.4",         #g.root-servers.net.
    "198.97.190.53",        #h.root-servers.net.
    "192.36.148.17",        #i.root-servers.net.
    "192.58.128.30",        #j.root-servers.net.
    "193.0.14.129",         #k.root-servers.net.
    "199.7.83.42",          #l.root-servers.net.
    "202.12.27.33"          #m.root-servers.net.
]

_DEBUG = 0

QTYPE = {1:'A', 2:'NS', 5:'CNAME', 6:'SOA', 10:'NULL', 12:'PTR', 13:'HINFO',
        15:'MX', 16:'TXT', 17:'RP', 18:'AFSDB', 24:'SIG', 25:'KEY',
        28:'AAAA', 29:'LOC', 33:'SRV', 35:'NAPTR', 36:'KX',
        37:'CERT', 38:'A6', 39:'DNAME', 41:'OPT', 42:'APL',
        43:'DS', 44:'SSHFP', 45:'IPSECKEY', 46:'RRSIG', 47:'NSEC',
        48:'DNSKEY', 49:'DHCID', 50:'NSEC3', 51:'NSEC3PARAM',
        52:'TLSA', 53:'HIP', 55:'HIP', 59:'CDS', 60:'CDNSKEY',
        61:'OPENPGPKEY', 62:'CSYNC', 63:'ZONEMD', 64:'SVCB',
        65:'HTTPS', 99:'SPF', 108:'EUI48', 109:'EUI64', 249:'TKEY',
        250:'TSIG', 251:'IXFR', 252:'AXFR', 255:'ANY', 256:'URI',
        257:'CAA', 32768:'TA', 32769:'DLV'}

CLASS = {1:'IN', 2:'CS', 3:'CH', 4:'Hesiod', 254:'None', 255:'*'}

class Recursive:

    def __init__(self, timeout:float = 1, depth:int = 30, retry:int = 3, extresolver:str = None):
        self.timeout = timeout
        self.depth = depth
        self.retry = retry
        self.extresolver = extresolver

    def recursive(self, query:dns.message.Message):
        resolver = self.extresolver
        # - External resolving if specify external DNS server
        if resolver:
            result = Recursive.extresolve(self, resolver, query)
            return result, None
        else:
            # - Internal resolving if it is empty
            try:
                start = time.time()
                for i in range(2):
                    result, auth = Recursive.resolve(self, query, _ROOT[i], 0)
                    if result and dns.flags.AA in result.flags: break
                if not result: raise Exception 
            except: # <-In any troubles at process resolving returns request with SERVFAIL code
                #logging.exception(f'Stage: Recursive: {query.question}')
                result = dns.message.make_response(query)
                result.set_rcode(4)
                auth = None
            finally:
                end = time.time()
                return result, auth, end-start

    def resolve(self, query:dns.message.QueryMessage, ns, depth):
        # -Checking current recursion depth-
        try:
            if depth >= self.depth: 
                raise Exception("Reach maxdetph - %s!" % self.depth)# <- Set max recursion depth
            depth += 1
            if _DEBUG in [1,3]: print(f"{depth}: {ns}") # <- SOME DEBUG
        except:
            result = dns.message.make_response(query)
            result.set_rcode(5)
            #logging.exception(f'Resolve: #1, qname - {result.question[0].name}')
            return result, ns
        
        # -Trying to get answer from specifing nameserver-
        try:
            for i in range(self.retry):
                try:
                    result = dns.query.udp(query, ns, self.timeout)
                    break
                except dns.exception.Timeout as e:
                    pass
                except:
                    result = dns.message.make_response(query)
                    result.set_rcode(4)
                    return result, ns
            if query.id != result.id:
                raise Exception('ID mismatch!')
            if not result: raise Exception
            if _DEBUG in [2,3]: print(result,'\n\n')  # <- SOME DEBUG
        except Exception:
            #logging.exception('RESOLVE')
            result = dns.message.make_response(query)
            result.set_rcode(2)
            return result, ns

        if dns.flags.AA in result.flags: 
            return result, ns # <- If got a rdata then return it
        
        if result.additional:
            random.shuffle(result.additional)
            for rr in result.additional:
                ns = str(rr[0])
                if ipaddress.ip_address(ns).version == 4:
                    result, ns = Recursive.resolve(self,query, ns, depth)
                    if result and result.rcode() in [
                        dns.rcode.NOERROR]: return result, ns
            return None, ns

        elif result.authority:
            random.shuffle(result.authority)
            for authlist in result.authority:
                for rr in authlist.processing_order():
                    qname = dns.name.from_text(str(rr))
                    nsquery = dns.message.make_query(qname, dns.rdatatype.A, dns.rdataclass.IN)
                    for ns in _ROOT:
                        nsdata, _ = Recursive.resolve(self, nsquery, ns, depth)
                        if nsdata.rcode() is dns.rcode.REFUSED: break
                        if not dns.rcode.NOERROR == nsdata.rcode():
                            continue
                        if nsdata.answer:
                            for rr in nsdata.answer:
                                ns = str(rr[0])
                                if ipaddress.ip_address(ns).version == 4:
                                    result, ns = Recursive.resolve(self, query, ns, depth)
                                if result and result.rcode() in [
                                    dns.rcode.NOERROR]: return result, ns
                            return None, ns
        return None, ns

    def extresolve(self, resolver, rdata):
        try:
            dns.query.send_udp(self.udp, rdata, (resolver, 53))
            answer,_ = dns.query.receive_udp(self.udp,(resolver, 53))
            print(answer)
        except:
            answer = dns.message.make_response(rdata)
            answer.set_rcode(2)
        return answer

           

class GetNS(threading.Thread):

    def __init__(self, query, nslist, depth):
        threading.Thread.__init__(self)
        self.result = None
        self.query = query
        self.nslist = nslist
        self.depth = depth
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.settimeout(1)
 
    def run(self):
        
        R = Recursive()
        self.result = R.resolve(self.query, self.nslist, self.depth, self.udp)

if __name__ == "__main__":
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    conf = {
        "resolver": None,
        "depth": 30
    }
    udp.bind(('127.0.0.2', 5300))
    R = Recursive(timeout=0.05)
    while True:
        data, client  = udp.recvfrom(512)
        message = dns.message.from_wire(data)
        answer, ns, rt = R.recursive(message)
        udp.sendto(answer.to_wire(message.origin),client)

