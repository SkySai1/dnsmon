#!./mon/bin/python3
import dns.resolver
import dns.exception
from sqlalchemy.orm import declarative_base, Session, scoped_session, sessionmaker
from sqlalchemy import Column, Float, Text, DateTime, SmallInteger, BigInteger, String, create_engine, insert, select, update, delete
import datetime
import json
from multiprocessing import Process
from scrapper import scrap
import os
import random
import sys
import time
import statistics


### Раздел описания сущностей БД 
def enginer():
    engine = create_engine("postgresql+psycopg2://dnschecker:moscow23./@localhost:5432/dnscheckerdev")
    return engine

base_engine = enginer()
Base = declarative_base()

class Domains(Base):  
    __tablename__ = "domains" 
    id = Column(BigInteger, primary_key=True)
    ts = Column(DateTime(timezone=True), nullable=False)  
    domain = Column(String(255), nullable=False, unique=True)  
    status = Column(SmallInteger, nullable=False)
    message = Column(Text)

class TimeResolve(Base):  
    __tablename__ = "timeresolve" 
    id = Column(BigInteger, primary_key=True)
    ts = Column(DateTime(timezone=True), nullable=False)  
    server = Column(String(255), nullable=False)  
    rtime = Column(Float)

class Servers(Base):  
    __tablename__ = "servers" 
    id = Column(BigInteger, primary_key=True)
    ts = Column(DateTime(timezone=True), nullable=False)  
    server = Column(String(255), nullable=False, unique=True)  
    status = Column(SmallInteger, nullable=False)
    message = Column(Text)

class Geo(Base):  
    __tablename__ = "geomap" 
    id = Column(BigInteger, primary_key=True)
    ts = Column(DateTime(timezone=True), nullable=False)  
    server = Column(String(255), nullable=False, unique=True)  
    status = Column(SmallInteger, nullable=False)
    message = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    region = Column(String(255))
    country = Column(String(255))


Base.metadata.create_all(base_engine)

### Раздел функций взаимодействия с БД ###
class AccessDB:

    def __init__(self, engine):
        self.engine = engine

    def domain_sync(self, d_list):
        with Session(self.engine) as conn:
            d_in_db = select(Domains.domain)
            result = conn.execute(d_in_db).fetchall()
            for d_db in result:
                exist = False
                for owner in d_list:
                    for d in d_list[owner]:
                        if d == d_db[0]:
                            exist = True
                            break
                if exist is False:
                    delt = delete(Domains).filter(Domains.domain == d_db[0])
                    conn.execute(delt)
                    conn.commit()
                    conn.close()                                                                                            
        

    def server_sync(self, ns_list):
        with Session(self.engine) as conn:
            ns_in_db = select(Servers.server)
            result = conn.execute(ns_in_db).fetchall()
            for ns_db in result:
                exist = False
                for ns in ns_list:
                    if ns == ns_db[0]:
                        exist = True
                        break
                if exist is False:
                    delt = delete(Servers).filter(Servers.server == ns_db[0])
                    conn.execute(delt)
                    conn.commit()
                    conn.close()

    def update_dtable(self, domain, status, message):
        with Session(self.engine) as conn:
            check = select(Domains).filter(Domains.domain == domain)
            result = conn.execute(check).fetchall()
            offset = datetime.timedelta(hours=3)
            tz = datetime.timezone(offset, name='MSC')
            now = datetime.datetime.now(tz=tz).strftime(f"%m/%d/%y %H:%M:%S")
            if result:
                if status == 1:
                    stmt = update(Domains).values(
                        ts = now, 
                        status = status,
                        message = message
                        ).where(Domains.domain == domain)
                else:
                    stmt = update(Domains).values(
                        status = status,
                        message = message
                        ).where(Domains.domain == domain)
            else:
                stmt = insert(Domains).values(
                    ts= now,
                    domain = domain,
                    status = status,
                    message = message
                    )
            conn.execute(stmt)
            conn.commit()
            conn.close()

    def update_nstime(self, ns, time):
        with Session(self.engine) as conn:
            offset = datetime.timedelta(hours=3)
            tz = datetime.timezone(offset, name='MSC')
            now = datetime.datetime.now(tz=tz).strftime(f"%m/%d/%y %H:%M:%S")
            stmt = insert(TimeResolve).values(
                ts = now,
                server = ns,
                rtime = time
            )
            conn.execute(stmt)
            conn.commit()
            conn.close()

    def update_stable(self, ns, status, message):
        with Session(self.engine) as conn:
            check = select(Servers).filter(Servers.server == ns)
            result = conn.execute(check).fetchall()
            offset = datetime.timedelta(hours=3)
            tz = datetime.timezone(offset, name='MSC')
            now = datetime.datetime.now(tz=tz).strftime(f"%m/%d/%y %H:%M:%S")
            if result:
                if status == 1:
                    stmt = update(Servers).values(
                        ts = now, 
                        status = status,
                        message = message
                        ).where(Servers.server == ns)
                else:
                    stmt = update(Servers).values(
                        status = status,
                        message = message
                        ).where(Servers.server == ns)               
            else:
                stmt = insert(Servers).values(
                    ts= now,
                    server = ns,
                    status = status,
                    message = message
                    )
            conn.execute(stmt)
            conn.commit()
            conn.close()

    def update_geomap(self, data):
        with Session(self.engine) as conn:
            check = select(Geo.status).filter(Geo.server == data['server'])
            result = conn.execute(check).fetchall()
            offset = datetime.timedelta(hours=3)
            tz = datetime.timezone(offset, name='MSC')
            now = datetime.datetime.now(tz=tz).strftime(f"%m/%d/%y %H:%M:%S")
            try:
                if result[0][0] == data['status']:
                    stmt = update(Geo).values(
                        status = data['status'],
                        message = data['message']
                        ).where(Geo.server == data['server'])
                elif result:
                    stmt = update(Geo).values(
                        ts = now, 
                        status = data['status'],
                        message = data['message']
                        ).where(Geo.server == data['server'])
            except:
                stmt = insert(Geo).values(
                    ts = now,
                    server = data['server'],
                    status = data['status'],
                    message = data['message'],
                    latitude = data['latitude'],
                    longitude = data['longitude'],
                    region = data['region'],
                    country = data['country']
                )
            conn.execute(stmt) #type: ignore
            conn.commit()
            conn.close()

    def geomap_sync(self, pub_list):
        with Session(self.engine) as conn:
            ns_in_db = select(Geo.server)
            result = conn.execute(ns_in_db).fetchall()
            for ns in result:
                exist = False    
                for region in pub_list:
                    for country in pub_list[region]:
                        for server in pub_list[region][country]:
                            if server == ns[0]:
                                exist = True
                                break
                if exist is False:
                    delt = delete(Geo).filter(Geo.server == ns[0])
                    conn.execute(delt)
                    conn.commit()
                    conn.close()

### Раздел функций поиск публичных NS ###
def nslook(ns_storage):
    for url in ns_storage:
        print(url)    

### Раздел функций проверки DNS ###
def stats(type, log, name, flag, engine):
    if type == 'check_d':
        data = []
        for line in log:
            data.append(line)
        message = "\n".join(data)
        update_dtable(name, flag, message, engine)
    elif type == 'ns_time':
        for ns in name:
            update_nstime(ns, name[ns], engine)
        


def get_list(path):
    if os.path.exists(path):
        with open(path, "r") as f:
            list = json.loads(f.read())
            return list

def domain_resolve(d, owner, ns_list, ns_time, engine):
    query = dns.resolver.Resolver()
    query.timeout = 2
    query.lifetime = 2
    ns_auth = []
    lines = []
    for ns in ns_list:
        if owner == ns_list[ns][1]:
            ns_auth.append(ns_list[ns][0])     
    try:
        random.shuffle(ns_auth)
        query.nameservers = ns_auth
        try: answer = query.resolve(d, "A")
        except: answer = query.resolve(d, "CNAME")
        ns_time[answer.nameserver] = answer.response.time
        status = 1
    except (Exception, dns.exception.Timeout) as e:
        lines.append(f"{d} - {str(e)}")
        status = 0
    stats('check_d', lines, d, status, engine)
    return ns_time

def resolve_time(data, ns_list, engine):
    ns_time = {}
    avg = []
    for ns in ns_list:
        try:
            for block in data:
                    avg.append(block[ns_list[ns][0]])
            ns_time[ns] = statistics.mean(avg)
        except: continue
    stats('ns_time', '', ns_time, '', engine)

def check_domains(d_list, ns_list, engine):
    ns_time = {}
    data = []
    for owner in d_list:
        for d in d_list[owner]:
            data.append(domain_resolve(d, owner, ns_list, ns_time, engine))
    resolve_time(data, ns_list, engine)

def check_ns(ns_list, db:AccessDB):
    query = dns.resolver.Resolver()
    for ns in ns_list:
        if ns_list[ns][1] == 'ilbb': d = 'online.vtb.ru'
        elif ns_list[ns][1] == 'lb': d ='cvpn.vtb.ru'
        else: d ='vtb.ru'
        try:
            query.nameservers = [ns_list[ns][0]]
            query.resolve(d, "A")
            db.update_stable(ns, 1, '')
        except Exception as e:
            db.update_stable(ns, 0, str(e))

def geo_available(pub_list, domains, db:AccessDB):
    query = dns.resolver.Resolver()
    query.lifetime = 1
    for region in pub_list:
        for country in pub_list[region]:
            for server in pub_list[region][country]:
                data = {}
                query.nameservers = [server]
                message = []
                for owner in domains:
                    d = random.choice(domains[owner])
                    try: 
                        answer = query.resolve(d, "A")
                        for val in answer:
                            print(f'{country} {server}: {d} ({val})')
                    except (Exception, dns.exception.Timeout) as e:
                        message.append(f"- {d} ({owner}): {str(e)}")
                        print(f'{country} {server}: {d} ({e})')
                data['status'] = len(message)
                data['server'] = server
                data['country'] = country
                data['region'] = region
                data['message'] = "\n".join(message)
                data['latitude'] = pub_list[region][country][server]['Latitude']
                data['longitude'] = pub_list[region][country][server]['Longitude']
                db.update_geomap(data)
                time.sleep(1)
                    

def default(ns_list, d_list):
    db = AccessDB(enginer())
    while True:
        db.server_sync(ns_list)
        db.domain_sync(d_list)
        check_ns(ns_list, db)
        check_domains(d_list,ns_list, db)
        time.sleep(60)

def geo_check(p_list, d_list):
    db = AccessDB(enginer())
    while True:
        db.geomap_sync(p_list)
        geo_available(p_list, d_list, db)
        time.sleep(10)

# Мультипроцессинг:
def Parallel(data):
    proc = []
    for pos in data:
        for fn in pos:
            if type(pos[fn]) is dict:
                p = Process(target=fn, kwargs=pos[fn])
                p.start()
                proc.append(p)
            else:
                p = Process(target=fn, args=pos[fn])
                p.start()
                proc.append(p)
    for p in proc:
        p.join()

if __name__ == "__main__":
    ns_path = f"{os.getcwd()}/jsons/nslist.json"
    d_path = f"{os.getcwd()}/jsons/domainslist.json"
    p_path = f"{os.getcwd()}/jsons/public.json"
    ns_storage_path = f"{os.getcwd()}/jsons/ns_storage.json"
    ns_list = get_list(ns_path)
    d_list = get_list(d_path)
    p_list = get_list(p_path)
    ns_storage = get_list(ns_storage_path)
    while True:
        data = [
            {default: [ns_list, d_list]},
            {geo_check: [p_list, d_list]},
            {nslook: [ns_storage]}
        ]
        Parallel(data)

        time.sleep(10)
    