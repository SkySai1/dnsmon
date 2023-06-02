import datetime
import sys
import logging
import uuid
import dns.rcode
from sqlalchemy.orm import declarative_base, Session, scoped_session, sessionmaker
from sqlalchemy import engine, Column, Float, Text, DateTime, SmallInteger, BigInteger, String, create_engine, insert, select, update, delete
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


class Domains(Base):  
    __tablename__ = "domains" 
    id = Column(BigInteger, primary_key=True)
    ts = Column(DateTime(timezone=True), nullable=False)  
    domain = Column(String(255), nullable=False, unique=True)  
    status = Column(SmallInteger, nullable=False)
    result = Column(Text)
    message = Column(Text)
    auth = Column(String(255), default = None)

class TimeResolve(Base):  
    __tablename__ = "timeresolve" 
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
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

class AccessDB:
    def __init__(self, _CONF):
        self.engine = enginer(_CONF)
        self.timedelta = _CONF['timedelta']
    
    def UpdateDomains(self, domain, error:dns.rcode, auth = None, result = None, message = None):
        with Session(self.engine) as conn:
            check = select(Domains).filter(Domains.domain == domain)
            check = conn.execute(check).fetchall()
            if check:
                if error == dns.rcode.NOERROR:
                    stmt = update(Domains).values(
                        ts = getnow(self.timedelta), 
                        status = 1,
                        auth = auth,
                        result = result,
                        message = message
                        ).where(Domains.domain == domain)
                else:
                    stmt = update(Domains).values(
                        status = 0,
                        auth = auth,
                        result = dns.rcode.to_text(error),
                        message = message
                        ).where(Domains.domain == domain)
            else:
                if error != dns.rcode.NOERROR:
                    status = 0
                    result = dns.rcode.to_text(error)
                else: status = 1
                stmt = insert(Domains).values(
                    ts= getnow(self.timedelta),
                    domain = domain,
                    auth = auth,
                    status = status,
                    result = result,
                    message = message
                    )
            conn.execute(stmt)
            conn.commit()
            conn.close()

    def GetDomain(self, domain=None):
        with Session(self.engine) as conn:
            if domain:
                stmt = select(Domains.domain).filter(Domains.domain == domain)
            else:
                stmt = select(Domains.domain)
            result = conn.execute(stmt).fetchall()
            return result
        
    def RemoveDomain(self, domain):
        with Session(self.engine) as conn:
            stmt = delete(Domains).filter(Domains.domain == domain)
            conn.execute(stmt)
            conn.commit()

    
def getnow(delta, rise = 0):
    offset = datetime.timedelta(hours=delta)
    tz = datetime.timezone(offset)
    now = datetime.datetime.now(tz=tz)
    return now + datetime.timedelta(0,rise) 
