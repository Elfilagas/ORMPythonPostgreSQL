import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship
import json

Base = declarative_base()

class Publisher(Base):
    __tablename__ = "publisher"
    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=100), unique=True, nullable=False)

class Book(Base):
    __tablename__ = "book"
    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=100), unique=True, nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False)
    publisher = relationship(Publisher, backref="book_publisher")

class Shop(Base):
    __tablename__ = "shop"
    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=100), unique=True, nullable=False)

class Stock(Base):
    __tablename__ = "stock"
    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)
    book = relationship(Book, backref="stock_book")
    shop = relationship(Shop, backref="stock_shop")

class Sale(Base):
    __tablename__ = "sale"
    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Float, nullable=False)
    date_sale = sq.Column(sq.DateTime, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)
    stock = relationship(Stock, backref="sale_stock")


def create_tables(engine):
    """Create all described tables"""
    Base.metadata.drop_all(engine) # for creation test, must be deleted
    Base.metadata.create_all(engine)

def data_fromJSON_toPSQL(session, file_name: str = 'fixtures/tests_data.json'):
    """Read data from JSON and write into PostgreSQL DB"""
    with open(file_name, 'r', encoding='UTF-8') as file:
        data = json.load(file)
    for record in data:
        class_name = {'publisher': Publisher, 
                      'shop': Shop, 
                      'book': Book, 
                      'stock': Stock, 
                      'sale': Sale
                      }[record.get('model')]
        session.add(class_name(id=record.get('pk'), **record.get('fields')))
    session.commit()

def print_sales_by_publisher(session, pub_name:str):
    """Print 'book name | shop name | sale price | sale date' for all books of publisher by name or id"""
    if pub_name.isnumeric():
        condition = Publisher.id == pub_name
    else:
        condition = Publisher.name.like(f'%{pub_name}%')
    q = session.query(Book, Shop, Sale).select_from(Publisher). \
        join(Book).join(Stock).join(Shop).join(Sale).filter(condition).order_by(Sale.date_sale)

    result = []
    for book, shop, sale in q:
        result.append((book.title, shop.name, sale.price, sale.date_sale))
    if result:
        for i in result:
            print(i[0].ljust(max(len(j[0]) for j in result)), 
                i[1].ljust(max(len(j[1]) for j in result)),
                str(i[2]).ljust(max(len(str(j[2])) for j in result)), 
                str(i[3]).ljust(max(len(str(j[3])) for j in result)),  sep=' | ')
    else:
        print("По указанному издателю, не найдено записей.")


