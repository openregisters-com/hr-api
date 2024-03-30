from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


import os
from dotenv import load_dotenv

load_dotenv()

DB_LOCATION = os.getenv("DB_LOCATION", "/root/structured_information.db")
SQLALCHEMY_DATABASE_URL = f"sqlite:///{DB_LOCATION}"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
