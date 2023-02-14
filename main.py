from dotenv import load_dotenv
import os
import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from models import create_tables, Publisher, Book, Shop, Stock, Sale


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

    q = session.query(Book.title, Shop.name, Sale.price, Sale.count, Sale.date_sale).\
      join(Publisher).join(Stock).join(Sale).join(Shop).filter(condition).order_by(Sale.date_sale)

    for book, shop, price, count, date in q:
        print(f'{book:<40} | {shop:<10} | {price*count:<8} | {date}')

def get_DSN_from_dotenv() -> str:
    """Read DSN data from .env file"""
    load_dotenv()
    DRIVER = os.getenv("DRIVER")
    LOGIN = os.getenv("LOGIN")
    PASSWORD = os.getenv("PASSWORD")
    HOST = os.getenv("HOST")
    PORT = os.getenv("PORT")
    BD = os.getenv("BD")
    return f'{DRIVER}://{LOGIN}:{PASSWORD}@{HOST}:{PORT}/{BD}'

def main():
    DSN = get_DSN_from_dotenv()
    engine = sqlalchemy.create_engine(DSN)
    create_tables(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    data_fromJSON_toPSQL(session)
    pub_name = input("Введите имя издателя или его id: ")
    print_sales_by_publisher(session, pub_name)

if __name__ == "__main__":
    main()