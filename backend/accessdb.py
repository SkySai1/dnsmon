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
    try:  
        engine = create_engine(
            f"postgresql+psycopg2://{_CONF['dbuser']}:{_CONF['dbpass']}@{_CONF['dbhost']}:{_CONF['dbport']}/{_CONF['dbname']}",
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
    domain = Column(String(255), nullable=False, unique = True)  
    status = Column(SmallInteger, nullable=False)
    result = Column(Text)
    message = Column(Text)
    auth = Column(String(255), default = None)

class Zones(Base):
    __tablename__ = "zones"
    id = Column(BigInteger, primary_key=True)
    node = Column(String(255), ForeignKey('nodes.node', ondelete='cascade'), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)
    zone = Column(String(255), nullable=False, unique = True)
    status = Column(Integer, nullable=False)
    serial = Column(Integer)
    message = Column(Text)

class ZonesResolve(Base):  
    __tablename__ = "zoneresolve" 
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    node = Column(String(255), ForeignKey('nodes.node', ondelete='cascade'), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)
    zone = Column(String(255), nullable=False)  
    rtime = Column(Float)

class NSresolve(Base):  
    __tablename__ = "nsresolve" 
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
    server = Column(String(255), nullable=False, unique = True)  
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

class Geo(Base):  
    __tablename__ = "geostate" 
    id = Column(BigInteger, primary_key=True)
    node = Column(String(255), ForeignKey('nodes.node', ondelete='cascade'), nullable=False)
    server = Column(String(255), ForeignKey('geobase.ip', ondelete='cascade'), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)  
    status = Column(SmallInteger, nullable=False)
    message = Column(Text)

class AccessDB:
    def __init__(self, _CONF):
        self.engine = enginer(_CONF)
        self.timedelta = _CONF['timedelta']
        self.node = _CONF['node']
    
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

    def InsertTimeresolve(self, data, isns:bool = True):
        if isns is True: Object = NSresolve
        else: Object = ZonesResolve

        with Session(self.engine) as conn:
            try:
                conn.execute(insert(Object), data)
                conn.commit()
            except:
                logging.exception('INSERT TIMERESOLVE:')

    def UpdateNS(self, ns, message = None):
        with Session(self.engine) as conn:
            try:
                check = (select(Servers)
                         .filter(Servers.server == ns)
                         .filter(Servers.node == self.node))
                check = conn.execute(check).fetchall()
                if check:
                    if not message:
                        stmt = (update(Servers).values(
                            ts = getnow(self.timedelta),
                            status = 1,
                            message = message
                            ).filter(Servers.server == ns)
                            .filter(Servers.node == self.node)
                        )
                    else:
                        stmt = (update(Servers).values(
                            status = 0,
                            message = message
                            ).filter(Servers.server == ns)
                            .filter(Servers.node == self.node)
                        )
                else:
                    if not message:
                        status = 1
                    else: status = 0
                    stmt = insert(Servers).values(
                        ts= getnow(self.timedelta),
                        node = self.node,
                        status = status,
                        server = ns,
                        message = message
                        )
                conn.execute(stmt)
                conn.commit()
                conn.close()
            except:
                logging.exception('UPDATE NAMESERVERS TABLE:')

    def UpdateDomains(self, domain, error:dns.rcode, auth = None, result = None, message = None):
        with Session(self.engine) as conn:
            try:
                check = (select(Domains)
                         .filter(Domains.domain == domain)
                         .filter(Domains.node == self.node))
                check = conn.execute(check).fetchall()
                if check:
                    if error == dns.rcode.NOERROR:
                        stmt = (update(Domains).values(
                            ts = getnow(self.timedelta),
                            status = 1,
                            auth = auth,
                            result = result,
                            message = message
                            ).filter(Domains.domain == domain)
                            .filter(Domains.node == self.node)
                        )
                    else:
                        stmt = (update(Domains).values(
                            status = 0,
                            auth = auth,
                            result = dns.rcode.to_text(error),
                            message = message
                            ).filter(Domains.domain == domain)
                            .filter(Domains.node == self.node)
                        )
                else:
                    if error != dns.rcode.NOERROR:
                        status = 0
                        result = dns.rcode.to_text(error)
                    else: status = 1
                    stmt = insert(Domains).values(
                        ts= getnow(self.timedelta),
                        node = self.node,
                        domain = domain,
                        auth = auth,
                        status = status,
                        result = result,
                        message = message
                        )
                conn.execute(stmt)
                conn.commit()
                conn.close()
            except:
                logging.exception('UPDATE DOMAINS TABLE:')

    def UpdateZones(self, zone, status, serial:int = None, message = None):
        with Session(self.engine) as conn:
            try:
                check = (select(Zones)
                         .filter(Zones.zone == zone)
                         .filter(Zones.node == self.node))
                check = conn.execute(check).fetchall()
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
                    else:
                        stmt = (update(Zones).values(
                            status = 0,
                            serial = serial,
                            message = message
                            ).filter(Zones.zone == zone)
                            .filter(Zones.node == self.node)
                        )
                else:
                    if status == 1: status = serial
                    stmt = insert(Zones).values(
                        ts= getnow(self.timedelta),
                        node = self.node,
                        zone = zone,
                        status = status,
                        serial = serial,
                        message = message
                        )
                conn.execute(stmt)
                conn.commit()
                conn.close()
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
    
def getnow(delta, rise = 0):
    offset = datetime.timedelta(hours=delta)
    tz = datetime.timezone(offset)
    now = datetime.datetime.now(tz=tz)
    return now + datetime.timedelta(0,rise) 
