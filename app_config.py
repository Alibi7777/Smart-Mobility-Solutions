# app_config.py
from sqlalchemy import create_engine
import psycopg2

# ==== EDIT THESE IF NEEDED ====
DB_NAME = "gauteng_db"
DB_USER = "postgres"
DB_PASSWORD = "1234"  # <-- your actual password
DB_HOST = "localhost"
DB_PORT = "5432"
DB_SCHEMA = "gauteng"
DATA_DIR = (
    "/Users/aligator/Downloads/GautengRoadsDatasetJun2025_2"  # folder with your CSVs
)
# ==============================


def get_engine():
    """Return SQLAlchemy engine for database connection"""
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, pool_pre_ping=True)


def get_connection():
    """Return psycopg2 connection object"""
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
