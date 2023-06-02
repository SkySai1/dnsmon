#!./mon/bin/python3
import os
import configparser
import sys
import uuid
_OPTIONS =[
    'node',
    'refresh',
    'timedelta',
    'debug',
    'zones',
    'domains',
    'nameservers',
    'publicns',
    'dbuser',
    'dbpass',
    'dbhost',
    'dbport',
    'dbname'
]

def getconf(path):
    config = configparser.ConfigParser()
    config.read(path)
    parsed = {}
    for section in config.sections():
        for key in config[section]:
            if key in _OPTIONS:
                parsed[key] = config[section][key]
    parsed = filter(parsed)
    return parsed

def filter(config):
    config['debug'] = int(config['debug'])
    config['zones'] = os.path.abspath(config['zones'])
    config['domains'] = os.path.abspath(config['domains'])
    config['nameservers'] = os.path.abspath(config['nameservers'])
    config['publicns'] = os.path.abspath(config['publicns'])
    config['timedelta'] = int(config['timedelta'])
    config['refresh'] = float(config['refresh'])
    return config


def createconf(where, what:configparser.ConfigParser):
    with open(where, 'w+') as f:
        what.write(f)

def deafultconf():
    config = configparser.ConfigParser()
    DBHost = str(input('Input HOSTNAME of your Data Base:\n'))    
    DBUser = str(input('Input USER of your Data Base:\n'))
    DBPass = str(input('Input PASSWORD of your Data Base\'s user:\n'))
    DBName = str(input('Input BASENAME of your Data Base\n'))
    config['DEFAULT'] = {
        'debug': 0,
        'timedelta': 3,
        'refresh': 10,
        'node': "%s"%uuid.uuid4()
    }
    config['FILES'] = {
        "zones": "./jsons/zones.example.json",
        "domains": "./jsons/domains.example.json",
        "nameservers": "./jsons/nslist.example.json",
        "publicns": "./jsons/ns_storage.json",
    }
    config['DATABASE'] = {
        "dbuser": DBUser,
        "dbpass": DBPass,
        "dbhost": DBHost,
        "dbport": 5432,
        "dbname": DBName
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
