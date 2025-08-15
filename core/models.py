from sqlalchemy import Column, Date, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class OutNetModel(Base):
    __tablename__ = "luxury_discount_tracking"

    brand = Column(String(255), primary_key=True)
    product_count = Column(Integer)
    avg_price = Column(Float)
    avg_rrp = Column(Float)
    avg_discount = Column(Float)
    inventory_value = Column(Float)
    date = Column(Date, primary_key=True)