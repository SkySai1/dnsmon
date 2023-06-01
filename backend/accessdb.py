import sys
import logging
import uuid
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
    message = Column(Text)

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