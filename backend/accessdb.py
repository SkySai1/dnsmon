import datetime
import sys
import logging
import uuid
import dns.rcode
import psycopg2.errors
import sqlalchemy.exc
from sqlalchemy.orm import declarative_base, Session, scoped_session, sessionmaker
from sqlalchemy import ForeignKey, engine, Integer, Column, Float, Text, DateTime, SmallInteger, BigInteger, String, create_engine, insert, select, update, delete
from sqlalchemy.dialects.postgresql import UUID

def enginer(_CONF):
    dbuser = _CONF['DATABASE']['dbuser']
    dbpass = _CONF['DATABASE']['dbpass']
    dbhost = _CONF['DATABASE']['dbhost']
    dbport = _CONF['DATABASE']['dbport']
    dbname = _CONF['DATABASE']['dbname']
    try:  
        engine = create_engine(
            "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
                dbuser, dbpass, dbhost, dbport, dbname
            ),
            connect_args={'connect_timeout': 5},
            pool_pre_ping=True
        )
        engine.connect()
        return engine
    except: 
        logging.exception('MAKE ENGINE TO DB')
        sys.exit()
        
Base = declarative_base()

class Nodes(Base):
    __tablename__ = "nodes"
    id = Column(Integer, primary_key=True)
    node = Column(String(255), unique=True)

class Domains(Base):  
    __tablename__ = "domains" 
    id = Column(BigInteger, primary_key=True)
    node = Column(String(255), ForeignKey('nodes.node', ondelete='cascade'), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)  
    domain = Column(String(255), nullable=False)  
    status = Column(SmallInteger, nullable=False)
    result = Column(Text)
    message = Column(Text)
    auth = Column(String(255), default = None)

class Zones(Base):
    __tablename__ = "zones"
    id = Column(BigInteger, primary_key=True)
    node = Column(String(255), ForeignKey('nodes.node', ondelete='cascade'), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)
    zone = Column(String(255), nullable=False)
    status = Column(Integer, nullable=False)
    serial = Column(Integer)
    message = Column(Text)

class FullResolve(Base):  
    __tablename__ = "fullresolve" 
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    node = Column(String(255), ForeignKey('nodes.node', ondelete='cascade'), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)
    zone = Column(String(255), nullable=False)  
    rtime = Column(Float)

class ShortResolve(Base):  
    __tablename__ = "shortresolve" 
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    node = Column(String(255), ForeignKey('nodes.node', ondelete='cascade'), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)
    server = Column(String(255), nullable=False)  
    rtime = Column(Float)

class Servers(Base):  
    __tablename__ = "servers" 
    id = Column(BigInteger, primary_key=True)
    node = Column(String(255), ForeignKey('nodes.node', ondelete='cascade'), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)  
    server = Column(String(255), nullable=False)  
    status = Column(SmallInteger, nullable=False)
    message = Column(Text)

class GeoBase(Base):
    __tablename__ = "geobase"
    id = Column(BigInteger, primary_key=True)
    ip = Column(String, nullable=False, unique = True)
    latitude = Column(Float) # <- First value in coordinates
    longitude = Column(Float) # <- Second value in coordinates
    country = Column(String)
    city = Column(String)

class GeoState(Base):  
    __tablename__ = "geostate" 
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    node = Column(String(255), ForeignKey('nodes.node', ondelete='cascade'), nullable=False)
    ip = Column(String(255), ForeignKey('geobase.ip', ondelete='cascade'), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)  
    domain = Column(String(255), nullable=False)
    state = Column(SmallInteger, nullable=False)
    result = Column(Text)

class Logs(Base):
    __tablename__ = "logs"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    node = Column(String(255), ForeignKey('nodes.node', ondelete='cascade'), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)
    level = Column(String(255), nullable = False)
    object = Column(String(255), nullable = False)
    message = Column(Text)  


class AccessDB:
    def __init__(self, _CONF, storage = None):
        self.engine = enginer(_CONF)
        self.storage = storage
        self.conf = _CONF
        self.expire = int(_CONF['DATABASE']['storagetime'])
        self.timedelta = int(_CONF['DATABASE']['timedelta'])
        self.keep = int(_CONF['GEO']['keep'])
        self.node = _CONF['DATABASE']['node']


    def start(self, dlist=None, zlist=None, nslist=None, onlygeo=False):
        with Session(self.engine) as self.conn:
            if onlygeo is False:
                # -- Creating new node if it doesnt
                try:
                    check = self.conn.execute(select(Nodes.node).filter(Nodes.node == self.node)).fetchone()
                    if not check:
                        self.conn.execute(
                            insert(Nodes).values(node = self.node)
                        )
                except:
                    logging.exception('CREATING NEW NODE:')

                # -- Synchronizing domains
                try:
                    dlist_from_db = AccessDB.GetDomain(self)
                    for d in dlist_from_db:
                        if not d[0] in dlist:
                            AccessDB.RemoveDomain(self, d[0])
                except:
                    logging.exception('DOMAINS SYNC:')

                # -- Synchronizing zones
                try:
                    zlist_from_db = AccessDB.GetZone(self)
                    zlist = [zone for group in zlist for zone in zlist[group]]
                    for z in zlist_from_db:
                        if not z[0] in make_fqdn(zlist):
                            print(z[0])
                            AccessDB.RemoveZone(self, z[0])
                except Exception:
                    logging.exception('ZONES SYNC:')

                # -- Synchronizing NS
                try:
                    nslist_from_db = AccessDB.GetNS(self)
                    nsnames = []
                    for addr in nslist:
                        nsnames.append(nslist[addr][0])

                    for ns in nslist_from_db:
                        if not ns[0] in nsnames:
                            AccessDB.RemoveNS(self, ns[0])
                except Exception:
                    logging.exception('NS SYNC:')
                self.conn.commit()

            # -- Getting GEO Base
            try:
                geobase = AccessDB.GetGeo(self)
                return geobase
            except Exception:
                logging.exception('GET GEO')


    def parse(self, storage = None):
        if storage: self.storage = storage
        with Session(self.engine) as self.conn:
            if 'launch_domain_check' in self.storage:
                if 'DOMAINS' in self.storage['launch_domain_check']:
                    AccessDB.UpdateDomains(self, self.storage['launch_domain_check']['DOMAINS'])
                if 'SHORTRESOLVE' in self.storage['launch_domain_check']:
                    AccessDB.InsertTimeresolve(self, self.storage['launch_domain_check']['SHORTRESOLVE'], True)
            
            if 'launch_ns_and_zones_check' in self.storage:
                if 'NS' in self.storage['launch_ns_and_zones_check']:
                    AccessDB.UpdateNS(self, self.storage['launch_ns_and_zones_check']['NS'])
                if 'ZONES' in self.storage['launch_ns_and_zones_check']:
                    AccessDB.UpdateZones(self, self.storage['launch_ns_and_zones_check']['ZONES'])
            
            if 'launch_zones_resolve' in self.storage:
                if 'FULLRESOLVE' in self.storage['launch_zones_resolve']:
                    AccessDB.InsertTimeresolve(self, self.storage['launch_zones_resolve']['FULLRESOLVE'], False)
    
            if 'geocheck' in self.storage:
                    AccessDB.InsertGeostate(self, self.storage['geocheck'])
            
            if 'initgeo' in self.storage:
                    AccessDB.InsertGeobase(self, self.storage['initgeo'])


    def InsertLogs(self, level, object, message):
        try:
            stmt = (insert(Logs).values(
                node = self.node,
                ts = getnow(self.timedelta, 0),
                level = level,
                object = object,
                message = message
            ))
            self.conn.execute(stmt)
            stmt = (delete(Logs)
                    .filter(Logs.ts <= getnow(self.timedelta, -self.expire))
                    .filter(Logs.node == self.node)
            )
            self.conn.execute(stmt)
        except:
            logging.exception('INSERT LOGS:')
    
    def InsertGeobase(self, data):
            try:
                self.conn.execute(insert(GeoBase), data)
                self.conn.commit()
            except sqlalchemy.exc.IntegrityError:
                pass
            except Exception as e:
                logging.exception('INSERT GEO BASE:')
    
    def InsertGeostate(self, data):
        try:
            if data:
                self.conn.execute(insert(GeoState), data)
                AccessDB.RemoveGeo(self)
        except:
            logging.exception('INSERT GEO STATE:')

    def InsertTimeresolve(self, data, is_short:bool = True):
        if is_short is True: Object = ShortResolve
        else: Object = FullResolve
        try:
            if data:
                self.conn.execute(insert(Object), data)
                stmt = (delete(Object)
                        .filter(Object.ts <= getnow(self.timedelta, -self.expire))
                        .filter(Object.node == self.node)
                )
                self.conn.execute(stmt)
                self.conn.commit()
        except:
            logging.exception('INSERT TIMERESOLVE:')

    def UpdateNS(self, nsstorage):
        try:
            for data in nsstorage:
                ns = data['ns']
                message = data['message']    
                check = (select(Servers.status, Servers.message)
                         .filter(Servers.server == ns)
                         .filter(Servers.node == self.node))
                check = self.conn.execute(check).fetchone()
                if check:
                    if not message:
                        stmt = (update(Servers).values(
                            ts = getnow(self.timedelta),
                            status = 1,
                            message = message
                            ).filter(Servers.server == ns)
                            .filter(Servers.node == self.node)
                        )
                        if check[0] == 0:
                            AccessDB.InsertLogs(self, 'INFO', 'nameserver', f"{ns} is OK.")
                    else:
                        stmt = (update(Servers).values(
                            status = 0,
                            message = message
                            ).filter(Servers.server == ns)
                            .filter(Servers.node == self.node)
                        )
                        if check[0] == 1 and check[1] != message:
                            AccessDB.InsertLogs(self, 'ERROR', 'nameserver', f"{ns} is BAD: {message}.")
                       
                else:
                    if not message:
                        status = 1
                    else: 
                        status = 0
                        AccessDB.InsertLogs(self, 'ERROR', 'nameserver', f"{ns} is BAD: {message}.")
                    stmt = insert(Servers).values(
                        ts= getnow(self.timedelta),
                        node = self.node,
                        status = status,
                        server = ns,
                        message = message
                        )
                self.conn.execute(stmt)
            self.conn.commit()
        except:
            logging.exception('UPDATE NAMESERVERS TABLE:')

    def UpdateDomains(self, dstorage):
        try:
            for data in dstorage:
                check = (select(Domains.status, Domains.result)
                            .filter(Domains.domain == data['domain'])
                            .filter(Domains.node == self.node))
                check = self.conn.execute(check).fetchone()
                if check:
                    if data['error'] == dns.rcode.NOERROR:
                        stmt = (update(Domains).values(
                            ts = getnow(self.timedelta),
                            status = 1,
                            auth = data['auth'],
                            result = data['rdata'],
                            message = dns.rcode.to_text(data['error'])
                            ).filter(Domains.domain == data['domain'])
                            .filter(Domains.node == self.node)
                        )
                        if check[0] != 1:
                            AccessDB.InsertLogs(self, 'INFO', 'domain', f"{data['domain']} is OK.")
                    else:
                        stmt = (update(Domains).values(
                            status = 0,
                            auth = data['auth'],
                            result = dns.rcode.to_text(data['error']),
                            message = dns.rcode.to_text(data['error'])
                            ).filter(Domains.domain == data['domain'])
                            .filter(Domains.node == self.node)
                        )
                        if check[0] == 1 and check[1] != dns.rcode.to_text(data['error']):
                            AccessDB.InsertLogs(self, 'ERROR', 'domain', f"{data['domain']} is bad: {dns.rcode.to_text(data['error'])}.")
                else:
                    if data['error'] is dns.rcode.NOERROR:
                        status = 1
                        result = data['rdata']
                    else: 
                        status = 0
                        result = data['error']
                        AccessDB.InsertLogs(self, 'ERROR', 'domain', f"{data['domain']} is bad: {dns.rcode.to_text(data['error'])}.")
                    stmt = insert(Domains).values(
                        ts= getnow(self.timedelta),
                        node = self.node,
                        domain = data['domain'],
                        auth = data['auth'],
                        status = status,
                        result = result,
                        message = dns.rcode.to_text(data['error']),
                        )
                self.conn.execute(stmt)
            self.conn.commit()
        except:
            logging.exception('UPDATE DOMAINS TABLE:')

    def UpdateZones(self, znstorage):
        try:
            for data in znstorage:
                zone = data['zone']
                status = data['status']
                serial = data['serial']
                message = data['message']
                check = (select(Zones.status, Zones.message)
                         .filter(Zones.zone == zone)
                         .filter(Zones.node == self.node))
                check = self.conn.execute(check).fetchone()
                if check:
                    if status == 1:
                        stmt = (update(Zones).values(
                            ts = getnow(self.timedelta),
                            status = serial,
                            serial = serial,
                            message = message
                            ).filter(Zones.zone == zone)
                            .filter(Zones.node == self.node)
                        )
                        if check[0] == 0:
                            AccessDB.InsertLogs(self, 'INFO', 'zone', f"{zone} is OK.")
                    else:
                        stmt = (update(Zones).values(
                            status = 0,
                            serial = serial,
                            message = message
                            ).filter(Zones.zone == zone)
                            .filter(Zones.node == self.node)
                        )
                        if check[0] != 0 and check[1] != message:
                            AccessDB.InsertLogs(self, 'WARNING', 'zone', f"{zone} is bad: {message}.")
                else:
                    if status == 1: 
                        status = serial
                    else:
                        AccessDB.InsertLogs(self, 'WARNING', 'zone', f"{zone} is bad: {message}.")
                    stmt = insert(Zones).values(
                        ts= getnow(self.timedelta),
                        node = self.node,
                        zone = zone,
                        status = status,
                        serial = serial,
                        message = message
                        )
                self.conn.execute(stmt)
            self.conn.commit()
        except:
            logging.exception('UPDATE ZONES TABLE:')

    def GetDomain(self, domain=None):
        try:
            if domain:
                stmt = (select(Domains.domain)
                        .filter(Domains.domain == domain)
                        .filter(Domains.node == self.node))
            else:
                stmt = select(Domains.domain)
            result = self.conn.execute(stmt).fetchall()
            return result
        except:
            logging.exception('GET DOMAINS FROM DB:')
        
    def RemoveDomain(self, domain):
        try:
            stmt = (delete(Domains)
                    .filter(Domains.domain == domain)
                    .filter(Domains.node == self.node))
            self.conn.execute(stmt)
        except:
            logging.exception('REMOVE DOMAIN FROM DB:')

    def GetZone(self, zone=None):
        try:
            if zone:
                stmt = (select(Zones.zone)
                        .filter(Zones.zone == zone)
                        .filter(Zones.node == self.node))
            else:
                stmt = select(Zones.zone)
            result = self.conn.execute(stmt).fetchall()
            return result
        except:
            logging.exception('GET ZONE FROM DB:')
        
    def RemoveZone(self, zone):
        try:
            stmt = (delete(Zones)
                    .filter(Zones.zone == zone)
                    .filter(Zones.node == self.node))
            self.conn.execute(stmt)
        except:
            logging.exception('REMOVE ZONE FROM DB:')

    def GetNS(self, server=None):
        try:
            if server:
                stmt = (select(Servers.server)
                        .filter(Servers.server == server)
                        .filter(Servers.node == self.node))
            else:
                stmt = select(Servers.server)
            result = self.conn.execute(stmt).fetchall()
            return result
        except:
            logging.exception('GET server FROM DB:')
        
    def RemoveNS(self, server):
        try:
            stmt = (delete(Servers)
                    .filter(Servers.server == server)
                    .filter(Servers.node == self.node))
            self.conn.execute(stmt)
        except:
            logging.exception('REMOVE server FROM DB:')
    
    def GetGeo(self):
        try:
            data = self.conn.execute(select(GeoBase)).fetchall()
            return data
        except:
            logging.exception('Get GEO from DB')

    def RemoveGeo(self):
        try:
            stmt = (delete(GeoState)
                    .filter(GeoState.ts <= getnow(self.timedelta, -self.keep))
                    .returning(GeoState.ip)
            )
            result = self.conn.scalars(stmt).all()
            self.conn.commit()
        except:
            logging.exception('Remove stats from GeoState')
    
def getnow(delta, rise = 0):
    offset = datetime.timedelta(hours=delta)
    tz = datetime.timezone(offset)
    now = datetime.datetime.now(tz=tz)
    return now + datetime.timedelta(0,rise) 

def make_fqdn(data):
    new_list = []
    for d in data:
        if '.' != d[-1]:
            d += '.'
        new_list.append(d)
    return new_list
