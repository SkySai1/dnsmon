#!./mon/bin/python3
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
    'DATABASE': ['node', 'dbuser', 'dbpass', 'dbhost', 'dbport', 'dbname', 'storage', 'timedelta'],
    'GEO': ['sleep', 'keep']
}

def getconf(path):
    config = configparser.ConfigParser()
    config.read(path)
    parsed = {}
    try:
        for section in _OPTIONS:
            if config.has_section(section) is not True: raise Exception(f'bad section - {section}')
            for key in _OPTIONS[section]:
                if config.has_option(section, key) is not True: raise Exception(f'bad key in config file - {key}')
        return config
    except:
        logging.exception('READ CONFIG FILE')

def createconf(where, what:configparser.ConfigParser):
    with open(where, 'w+') as f:
        what.write(f)

def deafultconf():
    config = configparser.ConfigParser()
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
        "publicns": "./jsons/ns_storage.json",
    }
    config['DATABASE'] = {
        "node": "%s"%uuid.uuid4(),
        "dbuser": DBUser,
        "dbpass": DBPass,
        "dbhost": DBHost,
        "dbport": 5432,
        "dbname": DBName,
        "storage": 2592000,
        "timedelta": 3,
    }
    config['GEO'] = {
        "sleep": 50,
        "keep": 600,
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
