#!./mon/bin/python3
import json
import random
import urllib.request
import nmap
import re
import dns.resolver


def getnslist(url):
    #url = 'https://public-dns.info/nameserver/nl.json'
    response = urllib.request.urlopen(url)
    data = json.loads(response.read())
    return data

def scanner(ip):
    port = 53
    scan = nmap.PortScanner()
    try:
        result = scan.scan(ip, str(port), '-Pn -sV -sU', sudo=True)
        #res = result['scan'][ip]['udp'][53]['state']
        #print(json.dumps(result, indent=4))
        return result['scan'][ip]['udp'][53]['state']
    except Exception as e:
        return 'closed'

def geocheck(ip):
    url = f'http://ipinfo.io/{ip}/json'
    response = urllib.request.urlopen(url)
    data = json.loads(response.read())
    #print(json.dumps(data, indent=4))
    location = {
        "coordinates": data["loc"].split(','),
        "city": data["city"],
        "country": data["country"]
    }
    return location

def scrap(url):
    data = getnslist(url)
    random.shuffle(data)
    for i in range(10):
        ip = data[i]['ip']
        location = geocheck(ip)
        city = location["city"]
        coordinates = location["coordinates"]
        if coordinates and re.match('^[0-9]+.[0-9]+.[0-9]+.[0-9]+$', ip): 
            state = scanner(ip)
            if state == 'open':
                try:
                    query = dns.resolver.Resolver()
                    query = dns.resolver.Resolver()
                    query.nameservers = [ip]
                    query.resolve('vtb.ru', "A")
                    print(f"{city} {ip} {coordinates}: OK")
                except Exception as e:
                    print(f"{city} {ip} {coordinates}: BAD")
            else: print(f"{city} {ip}: closed")

#scrap()
#print(scanner("82.196.13.196"))
#geocheck("82.196.13.196")