import psycopg2
import os
import logging
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL")
logger = logging.getLogger(__name__)

def init_db():
    """Initializes the database and creates the necessary tables."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        # Table for successful uploads
        cur.execute("""
            CREATE TABLE IF NOT EXISTS uploaded_dramas (
                id SERIAL PRIMARY KEY,
                title TEXT UNIQUE NOT NULL,
                book_id TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Table for failures
        cur.execute("""
            CREATE TABLE IF NOT EXISTS drama_failures (
                title TEXT PRIMARY KEY,
                failure_count INT DEFAULT 1,
                last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    """Marks a drama as uploaded in the database and clears its failure record if any."""
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
        # Clear failure record if successful
        cur.execute("DELETE FROM drama_failures WHERE title = %s", (title,))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Database insert error: {e}")
        return False

def record_failure(title):
    """Increments the failure count for a drama title."""
    if not DATABASE_URL:
        return False
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO drama_failures (title, failure_count, last_attempt)
            VALUES (%s, 1, CURRENT_TIMESTAMP)
            ON CONFLICT (title) DO UPDATE
            SET failure_count = drama_failures.failure_count + 1,
                last_attempt = CURRENT_TIMESTAMP
        """, (title,))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Failed to record failure for {title}: {e}")
        return False

def get_failure_count(title):
    """Returns the number of times a drama title has failed."""
    if not DATABASE_URL:
        return 0
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT failure_count FROM drama_failures WHERE title = %s", (title,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0] if row else 0
    except Exception as e:
        logger.error(f"Failed to get failure count for {title}: {e}")
        return 0
