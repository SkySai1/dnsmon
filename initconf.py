#!/etc/dev/dnschecker/mon/bin/python3
import os
import configparser
import sys
import uuid
import logging
_OPTIONS ={
    'GENERAL': ['debug', 'maxthreads'],
    'RESOLVE': ['timeout', 'retry'],
    'RECURSION': ['timeout', 'maxdepth', 'retry'],
    'FILES': ['zones', 'domains', 'nameservers', 'publicns'],
    'DATABASE': ['engine','node', 'dbuser', 'dbpass', 'dbhost', 'dbport', 'dbname', 'storagetime', 'timedelta'],
    'GEO': ['maxcities', 'maxservers', 'retry', 'timeout', 'sleep', 'keep', 'initcount']
}

def getconf(path):
    config = configparser.ConfigParser()
    config.read(path)
    bad = []
    try:
        for section in _OPTIONS:
            for key in _OPTIONS[section]:
                if config.has_option(section, key) is not True: bad.append(f'Bad config file: missing key - {key} in {section} section')
        if bad: raise Exception("\n".join(bad))
        return config
    except Exception as e:
        print(e)
        sys.exit()

def createconf(where, what:configparser.ConfigParser):
    with open(where, 'w+') as f:
        what.write(f)

def deafultconf():
    config = configparser.ConfigParser(allow_no_value=True)
    DBHost = str(input('Input HOSTNAME of your Data Base:\n'))    
    DBUser = str(input('Input USER of your Data Base:\n'))
    DBPass = str(input('Input PASSWORD of your Data Base\'s user:\n'))
    DBName = str(input('Input BASENAME of your Data Base\n'))
    config['GENERAL'] = {
        'debug': 0,
        'maxthreads': 10,
    }
    config['RESOLVE'] = {
        'timeout': 0.3,
        'retry': 3
    }
    config['RECURSION'] = {
        "timeout": 0.1,
        "maxdepth": 30,
        "retry": 3
    }
    config['FILES'] = {
        "zones": "./jsons/zones.example.json",
        "domains": "./jsons/domains.example.json",
        "nameservers": "./jsons/nslist.example.json",
        "healthcheck": "./jsons/healthcheck.example.json",
        "publicns": "./jsons/ns_storage.json",
    }
    config['DATABASE'] = {
        ";Possible values of engine: pgsql, mysql": None,
        "engine" : 'pgsql',
        "node": "%s"%uuid.uuid4(),
        "dbuser": DBUser,
        "dbpass": DBPass,
        "dbhost": DBHost,
        "dbport": 5432,
        "dbname": DBName,
        ";Time to keeping data in seconds": None,
        "storagetime": 2592000,
        ";for mysql better keep timedelta as 0, for pgsql as your region timezone": None,
        "timedelta": 3,
    }
    config['GEO'] = {
        "maxcities": 2,
        "maxservers": 2,
        'retry': 2,
        "timeout": 1,
        "sleep": 50,
        "keep": 600,
        "initcount": 100
    }
    return config

if __name__ == "__main__":
    here = f"{os.path.abspath('./')}/config.conf"
    if os.path.exists(here):
            while True:
                try:
                    y = str(input(f"{here} is exists, do you wanna to recreate it? (y/n)\n"))
                    if y == "n": sys.exit()
                    elif y == "y": break
                except ValueError:
                    pass
                except KeyboardInterrupt:
                    sys.exit()
    conf = deafultconf()
    createconf(here, conf)
    getconf(here)
