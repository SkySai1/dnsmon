#!./mon/bin/python3
import json
import urllib.request
import nmap
import re
import dns.resolver


def getnslist():
    url = 'https://public-dns.info/nameserver/nl.json'
    response = urllib.request.urlopen(url)
    data = json.loads(response.read())
    return data
    #print(json.dumps(data, indent=4))
    for i in data:
        ip = i['ip']
        city = i['city']
        if city: print(f'{city}: {ip}')

def scanner(ip):
    port = 53
    scan = nmap.PortScanner()
    try:
        result = scan.scan(ip, str(port), '-Pn -sV -sU', sudo=True, timeout=5)
        #res = result['scan'][ip]['udp'][53]['state']
        #print(json.dumps(result, indent=4))
        return result['scan'][ip]['udp'][53]['state']
    except Exception as e:
        return 'closed'

def scrap():
    data = getnslist()
    for i in data:
        ip = i['ip']
        city = i['city']
        if city and re.match('^[0-9]+.[0-9]+.[0-9]+.[0-9]+$', ip): 
            state = scanner(ip)
            if state == 'open':
                try:
                    query.lifetime = 10
                    query = dns.resolver.Resolver()
                    query.nameservers = [ip]
                    query.resolve('vtb.ru', "A")
                    print(f"{city} {ip}: OK")
                except:
                    print(f"{city} {ip}: BAD")
            else: print(f"{city} {ip}: closed")

scrap()