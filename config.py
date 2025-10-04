from sqlalchemy import create_engine
import psycopg2


# from config import get_engine, get_connection

# engine = get_engine()
# conn = get_connection()


DB_NAME = "gauteng_db"
DB_USER = "postgres"
DB_PASSWORD = "1234"
DB_HOST = "localhost"
DB_PORT = "5432"


# SQLAlchemy engine
def get_engine():
    """Return SQLAlchemy engine for database connection"""
    connection_url = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(connection_url)


def get_connection():
    """Return psycopg2 connection object"""
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
