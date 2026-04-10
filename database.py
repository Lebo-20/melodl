import psycopg2
import os
import logging
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL")
logger = logging.getLogger(__name__)

def init_db():
    """Initializes the database and creates the necessary table."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS uploaded_dramas (
                id SERIAL PRIMARY KEY,
                title TEXT UNIQUE NOT NULL,
                book_id TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")

def is_drama_uploaded(title):
    """Checks if a drama title has already been uploaded."""
    if not DATABASE_URL:
        return False
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT id FROM uploaded_dramas WHERE title = %s", (title,))
        exists = cur.fetchone() is not None
        cur.close()
        conn.close()
        return exists
    except Exception as e:
        logger.error(f"Database check error: {e}")
        return False

def add_uploaded_drama(title, book_id=None):
    """Marks a drama as uploaded in the database."""
    if not DATABASE_URL:
        return False
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        # Use ON CONFLICT DO NOTHING to avoid errors on duplicate attempts
        cur.execute(
            "INSERT INTO uploaded_dramas (title, book_id) VALUES (%s, %s) ON CONFLICT (title) DO NOTHING",
            (title, book_id)
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Database insert error: {e}")
        return False
