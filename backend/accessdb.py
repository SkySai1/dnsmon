import datetime
import sys
import logging
import uuid
import dns.rcode
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
    domain = Column(String(255), nullable=False)  
    status = Column(SmallInteger, nullable=False)
    result = Column(Text)
    message = Column(Text)
    auth = Column(String(255), default = None)

class TimeResolve(Base):  
    __tablename__ = "timeresolve" 
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    node = Column(String(255), ForeignKey('nodes.node', ondelete='cascade'), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)
    server = Column(String(255), nullable=False)  
    rtime = Column(Float)
    rtime_short = Column(Float)

class Servers(Base):  
    __tablename__ = "servers" 
    id = Column(BigInteger, primary_key=True)
    node = Column(String(255), ForeignKey('nodes.node', ondelete='cascade'), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)  
    server = Column(String(255), nullable=False)  
    status = Column(SmallInteger, nullable=False)
    message = Column(Text)

class Geo(Base):  
    __tablename__ = "geomap" 
    id = Column(BigInteger, primary_key=True)
    node = Column(String(255), ForeignKey('nodes.node', ondelete='cascade'), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)  
    server = Column(String(255), nullable=False)  
    status = Column(SmallInteger, nullable=False)
    message = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    region = Column(String(255))
    country = Column(String(255))

class AccessDB:
    def __init__(self, _CONF):
        self.engine = enginer(_CONF)
        self.timedelta = _CONF['timedelta']
        self.node = _CONF['node']
    
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

    def InsertTimeresolve(self, data):
        with Session(self.engine) as conn:
            try:
                conn.execute(insert(TimeResolve), data)
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

    
def getnow(delta, rise = 0):
    offset = datetime.timedelta(hours=delta)
    tz = datetime.timezone(offset)
    now = datetime.datetime.now(tz=tz)
    return now + datetime.timedelta(0,rise) 
