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

    def __init__(self, timeout = 5, depth = 30, extresolver = None):
        self.timeout = timeout
        self.depth = depth
        self.extresolver = extresolver

    def recursive(self, query:dns.message.Message):
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # < - Init Recursive socket
        self.udp.settimeout(self.timeout) # < - Setting timeout

        # -- External resolving if specify external DNS server
        if self.extresolver:
            result = Recursive.extresolve(self, self.extresolver, query)
            return result, None
        
        # -- Internal resolving if it is empty
        try:
            start = time.time()
            result, ns = Recursive.resolve(self, query, _ROOT, 0, self.udp)
            end = time.time()
            if not result: raise Exception 
        except: # <-In any troubles at process resolving returns request with SERVFAIL code
            #logging.exception('RESOLVING')
            result = dns.message.make_response(query)
            result.set_rcode(2)
            ns = None
        finally:
            end = time.time()
            return  result, ns, end-start# <- In anyway returns byte's packet and DNS Record data

    def extresolve(self, resolver, rdata):
        try:
            dns.query.send_udp(self.udp, rdata, (resolver, 53))
            answer,_ = dns.query.receive_udp(self.udp,(resolver, 53))
            print(answer)
        except:
            answer = dns.message.make_response(rdata)
            answer.set_rcode(2)
        return answer

    def resolve(self, rdata:dns.message.QueryMessage, nslist, depth, udp:socket.socket = None):
        if type(nslist) is not list:
            nslist = [nslist] # < - Create list of NameServers if it doesnt
        random.shuffle(nslist)
        #print(nslist)
        for ns in nslist:
            # -Checking current recursion depth-
            try:
                if depth >= self.depth: 
                    raise Exception("Reach maxdetph - %s!" % self.depth)# <- Set max recursion depth
                depth += 1
                if _DEBUG == 1: print(f"{depth}: {ns}") # <- SOME DEBUG
            except:
                result = dns.message.make_response(rdata)
                result.set_rcode(2)
                #logging.exception(f'Resolve: #1, qname - {result.question[0].name}')
                return result, None
            
            # -Trying to get answer from authority nameserver-
            try:
                result = dns.query.udp(rdata,ns,1)
                if rdata.id != result.id:
                   raise dns.exception.DNSException('ID mismatch!')
                # -- SOME DEBUG
                if _DEBUG == 1: 
                    print(f"From {ns}:")
                    print(result.question[0])  
                    if result.answer: 
                        for a in result.answer:
                            print(a)
                    else:
                        for a in result.additional:
                            print(a)
                    print('\n\n')
            except dns.exception.Timeout:
                continue
            except dns.exception.DNSException:
                logging.exception(f'Resolve: #2')
                continue


            if result.answer:
                return result, ns # <- If got a rdata then return it
            elif not result or not result.authority: # <- if not and there is no authority NS then domain doesnt exist
                result.set_rcode(3) 
                return result, None
            
            NewNSlist = [] # <- IP list for authority NS
            if result.additional:
                for rr in result.additional:
                    ip = ipaddress.ip_address(str(rr[0]))
                    if ip.version == 4:
                        NewNSlist.append(str(ip))

            if not NewNSlist:
                stream = []
                for rr in result.authority[0]:
                    qname = dns.name.from_text(str(rr))
                    nsQuery = dns.message.make_query(qname, dns.rdatatype.A, dns.rdataclass.IN)
                    t = GetNS(nsQuery, _ROOT, depth)
                    t.start()
                    stream.append(t)
                for t in stream:
                    t.join()
                    try:
                        NSdata, _ = t.result
                        if NSdata.rcode == dns.rcode.REFUSED:
                            continue
                        if NSdata.answer:
                            for rr in NSdata.answer:
                                NewNSlist.append(str(rr[0]))
                    except Exception as e:
                        print(e)
                        #logging.exception('Resolve #3:') 
                        continue
                '''for rr in result.authority[0]:
                    qname = dns.name.from_text(str(rr))
                    nsQuery = dns.message.make_query(qname, dns.rdatatype.A, dns.rdataclass.IN)
                    NSdata, _ = Recursive.resolve(self, nsQuery, _ROOT, self.udp, depth)
                    try: 
                        if NSdata.rcode == dns.rcode.REFUSED:
                            continue
                        if NSdata.answer:
                            print(NSdata.answer)
                            for rr in NSdata.answer:
                                NewNSlist.append(str(rr[0]))
                            #break
                    except:
                        logging.exception('Resolve #3:') 
                        continue'''
            if NewNSlist:
                NewResult = Recursive.resolve(self, rdata, NewNSlist, depth, udp)
                return NewResult
            else:
                result.set_rcode(3)
                return result, ns
           

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
