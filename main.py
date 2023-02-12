from dotenv import load_dotenv
import os
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from models import create_tables, data_fromJSON_toPSQL, print_sales_by_publisher


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