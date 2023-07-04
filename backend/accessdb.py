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
    state = Column(SmallInteger, nullable=False)

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
        self.expire = int(_CONF['DATABASE']['storage'])
        self.timedelta = int(_CONF['DATABASE']['timedelta'])
        self.node = _CONF['DATABASE']['node']

    def parse(self):
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
    
    def InsertGeobase(self, ip, lat, long, city, country):
        with Session(self.engine) as conn:
            try:
                stmt = (insert(GeoBase).values(
                    ip = ip,
                    latitude = lat,
                    longitude = long,
                    city = city,
                    country = country
                ))
                conn.execute(stmt)
                conn.commit()

            except sqlalchemy.exc.IntegrityError:
                pass
            except Exception as e:
                logging.exception('INSERT GEO BASE:')
    
    def InsertGeostate(self, ip, state):
        with Session(self.engine) as conn:
            try:
                check = conn.execute(select(GeoState.ip).filter(GeoState.ip == ip)).first()
                if check:
                    stmt = (update(GeoState)
                            .filter(GeoState.ip == ip)
                            .values(
                                ts = getnow(self.timedelta, 0),
                                state = state
                            )
                    )
                else:
                    stmt = (insert(GeoState).values(
                        node = self.node,
                        ts = getnow(self.timedelta, 0),
                        ip = ip,
                        state = state
                    ))
                conn.execute(stmt)
                conn.commit()
            except:
                logging.exception('INSERT GEO STATE:')

    def NewNode(self):
        with Session(self.engine) as conn:
            try:
                check = conn.execute(select(Nodes.node).filter(Nodes.node == self.node)).fetchone()
                if not check:
                    conn.execute(
                        insert(Nodes).values(node = self.node)
                    )
                    conn.commit()
            except:
                logging.exception('CREATING NEW NODE:')

    def InsertTimeresolve(self, data, is_short:bool = True):
        if is_short is True: Object = ShortResolve
        else: Object = FullResolve
        try:
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
        with Session(self.engine) as conn:
            try:
                if domain:
                    stmt = (select(Domains.domain)
                            .filter(Domains.domain == domain)
                            .filter(Domains.node == self.node))
                else:
                    stmt = select(Domains.domain)
                result = conn.execute(stmt).fetchall()
                return result
            except:
                logging.exception('GET DOMAINS FROM DB:')
        
    def RemoveDomain(self, domain):
        with Session(self.engine) as conn:
            try:
                stmt = (delete(Domains)
                        .filter(Domains.domain == domain)
                        .filter(Domains.node == self.node))
                conn.execute(stmt)
                conn.commit()
            except:
                logging.exception('REMOVE DOMAIN FROM DB:')

    def GetZone(self, zone=None):
        with Session(self.engine) as conn:
            try:
                if zone:
                    stmt = (select(Zones.zone)
                            .filter(Zones.zone == zone)
                            .filter(Zones.node == self.node))
                else:
                    stmt = select(Zones.zone)
                result = conn.execute(stmt).fetchall()
                return result
            except:
                logging.exception('GET ZONE FROM DB:')
        
    def RemoveZone(self, zone):
        with Session(self.engine) as conn:
            try:
                stmt = (delete(Zones)
                        .filter(Zones.zone == zone)
                        .filter(Zones.node == self.node))
                conn.execute(stmt)
                conn.commit()
            except:
                logging.exception('REMOVE ZONE FROM DB:')

    def GetNS(self, server=None):
        with Session(self.engine) as conn:
            try:
                if server:
                    stmt = (select(Servers.server)
                            .filter(Servers.server == server)
                            .filter(Servers.node == self.node))
                else:
                    stmt = select(Servers.server)
                result = conn.execute(stmt).fetchall()
                return result
            except:
                logging.exception('GET server FROM DB:')
        
    def RemoveNS(self, server):
        with Session(self.engine) as conn:
            try:
                stmt = (delete(Servers)
                        .filter(Servers.server == server)
                        .filter(Servers.node == self.node))
                conn.execute(stmt)
                conn.commit()
            except:
                logging.exception('REMOVE server FROM DB:')
    
    def GetGeo(self):
        with Session(self.engine) as conn:
            try:
                data = conn.execute(select(GeoBase)).fetchall()
                return data
            except:
                logging.exception('Get GEO from DB')

    def RemoveGeo(self):
        with Session(self.engine) as conn:
            try:
                stmt = (delete(GeoState)
                        .filter(GeoState.ts <= getnow(self.timedelta, -self.conf['keep']))
                        .returning(GeoState.ip)
                )
                result = conn.scalars(stmt).all()
                conn.commit()
            except:
                logging.exception('Remove stats from GeoState')
    
def getnow(delta, rise = 0):
    offset = datetime.timedelta(hours=delta)
    tz = datetime.timezone(offset)
    now = datetime.datetime.now(tz=tz)
    return now + datetime.timedelta(0,rise) 
