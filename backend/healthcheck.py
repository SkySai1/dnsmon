
import logging
from threading import Thread, BoundedSemaphore
from urllib.request import Request, urlopen
from backend.recursive import Recursive
import dns.message
import dns.rdatatype
import ipaddress

class HealthCheck(Thread):

    def __init__(self, limit:BoundedSemaphore, _CONF, domain, methods):
        Thread.__init__(self)
        self.limit = limit
        self.timeout = float(_CONF['HEALTHCHECK']['timeout'])
        self.retry = int(_CONF['HEALTHCHECK']['retry'])
        self.maxdepth = int(_CONF['RECURSION']['maxdepth'])
        self.domain = domain
        if type(methods) is not list: methods = [methods]
        self.methods = methods
 
    def run(self):
        try:
            errors = []
            query = dns.message.make_query(self.domain, dns.rdatatype.A)
            addresses=[]
            R = Recursive()
            answer,_,_ = R.recursive(query)
            if answer and type(answer) is dns.message.QueryMessage:
                for rr in answer.answer:
                    for addr in rr: 
                        try:
                            ipaddress.ip_address(str(addr))
                            addresses.append(str(addr))
                        except: continue
            if addresses:
                for addr in addresses:
                    headers = {
                        "Connection": "keep-alive",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                        "Accept-Encoding": "gzip,",
                        #"Host": "online.vtb.ru",
                        "Accept-Language": "en;q=0.9,zh;q=0.8",
                        "Referer": "https://yandex.ru/",
                        "User-Agent": "Mozilla/5.0 (platform; rv:geckoversion) Gecko/geckotrail Firefox/firefoxversion",
                        "sec-ch-ua": "\"Chromium\";v=\"112\",",
                        "sec-ch-ua-mobile": "?0",
                        "sec-ch-ua-platform": "\"Windows\"",
                        "DNT": "1",
                        "Upgrade-Insecure-Requests": "1",
                        "Sec-Fetch-Site": "cross-site",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-User": "?1",
                        "Sec-Fetch-Dest": "document"
                    }
                    try:
                        if 'http' in self.methods:
                            print(addr, self.domain)
                            req = Request('http://%s' % addr)
                            req.headers = headers
                            req.host=self.domain
                            content = urlopen(req,timeout=self.timeout)
                        #print(self.domain, addresses, self.methods)
                    except Exception as e:
                        errors.append(f'method: HTTP, ip: {addr}: {str(e)}') 
                    try:
                        if 'https' in self.methods:
                            req = Request('https://%s' % addr)
                            req.host=self.domain
                            req.headers = headers
                            content = urlopen(req,timeout=self.timeout)
                    except Exception as e:
                        errors.append(f'method: HTTPS, ip: {addr}: {str(e)}')  
                        
            if errors: status = 0
            else: status = 1

            self.result = {
                'domain': self.domain,
                'address': addresses,
                'status': status,
                'message': errors
            }
            self.limit.release()
        except:
            logging.exception("HEALTHCHECK")