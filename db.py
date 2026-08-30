import psycopg2
import psycopg2.extras
from psycopg2 import pool as pg_pool
import os

_pool = None


def init_pool():
    global _pool
    _pool = pg_pool.ThreadedConnectionPool(
        minconn=2,
        maxconn=10,
        host=os.environ.get('POSTGRES_HOST'),
        port=os.environ.get('POSTGRES_PORT', 5432),
        dbname=os.environ.get('POSTGRES_DB'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'),
        cursor_factory=psycopg2.extras.RealDictCursor
    )


class _PooledConnection:
    def __init__(self, real_conn):
        self._real_conn = real_conn

    def cursor(self, *args, **kwargs):
        return self._real_conn.cursor(*args, **kwargs)

    def commit(self):
        self._real_conn.commit()

    def rollback(self):
        self._real_conn.rollback()

    def close(self):
        _pool.putconn(self._real_conn)


def get_db_connection():
    if _pool is None:
        init_pool()
    return _PooledConnection(_pool.getconn())


def get_donator_by_id(donator_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name, email FROM donators WHERE id = %s", (donator_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def init_forum_messages_table():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS forum_messages (
            id SERIAL PRIMARY KEY,
            channel TEXT NOT NULL,
            sender_id TEXT NOT NULL,
            sender_name TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


def get_channel_history(channel, limit=100):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, channel, sender_id, sender_name, message, created_at
        FROM forum_messages
        WHERE channel = %s
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (channel, limit)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return list(reversed(rows))


def save_channel_message(channel, sender_id, sender_name, message):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO forum_messages (channel, sender_id, sender_name, message)
        VALUES (%s, %s, %s, %s)
        RETURNING id, channel, sender_id, sender_name, message, created_at
        """,
        (channel, sender_id, sender_name, message)
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return row


def init_intro_threads_tables():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS intro_threads (
            id SERIAL PRIMARY KEY,
            donator_id INTEGER NOT NULL UNIQUE,
            author_name TEXT NOT NULL,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS intro_replies (
            id SERIAL PRIMARY KEY,
            thread_id INTEGER NOT NULL REFERENCES intro_threads(id) ON DELETE CASCADE,
            donator_id INTEGER NOT NULL,
            author_name TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


def get_intro_threads(page=1, per_page=10):
    offset = (page - 1) * per_page
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS total FROM intro_threads")
    total = cur.fetchone()['total']
    cur.execute(
        """
        SELECT t.id, t.donator_id, t.author_name, t.title, t.body, t.created_at,
               COUNT(r.id) AS reply_count
        FROM intro_threads t
        LEFT JOIN intro_replies r ON r.thread_id = t.id
        GROUP BY t.id
        ORDER BY t.created_at DESC
        LIMIT %s OFFSET %s
        """,
        (per_page, offset)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows, total


def get_intro_thread_by_donator(donator_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, donator_id, author_name, title, body, created_at FROM intro_threads WHERE donator_id = %s",
        (donator_id,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def upsert_intro_thread(donator_id, author_name, title, body):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO intro_threads (donator_id, author_name, title, body)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (donator_id)
        DO UPDATE SET author_name = EXCLUDED.author_name, title = EXCLUDED.title, body = EXCLUDED.body
        RETURNING id, donator_id, author_name, title, body, created_at
        """,
        (donator_id, author_name, title, body)
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return row


def get_intro_thread_owner(thread_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT donator_id FROM intro_threads WHERE id = %s", (thread_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row['donator_id'] if row else None


def delete_intro_thread_by_id(thread_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM intro_threads WHERE id = %s", (thread_id,))
    deleted = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return deleted


def get_intro_replies(thread_id, page=1, per_page=10):
    offset = (page - 1) * per_page
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS total FROM intro_replies WHERE thread_id = %s", (thread_id,))
    total = cur.fetchone()['total']
    cur.execute(
        """
        SELECT id, thread_id, donator_id, author_name, message, created_at
        FROM intro_replies WHERE thread_id = %s
        ORDER BY created_at ASC LIMIT %s OFFSET %s
        """,
        (thread_id, per_page, offset)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows, total


def add_intro_reply(thread_id, donator_id, author_name, message):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO intro_replies (thread_id, donator_id, author_name, message)
        VALUES (%s, %s, %s, %s)
        RETURNING id, thread_id, donator_id, author_name, message, created_at
        """,
        (thread_id, donator_id, author_name, message)
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return row


def get_intro_reply_owner(reply_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT donator_id FROM intro_replies WHERE id = %s", (reply_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row['donator_id'] if row else None


def delete_intro_reply_by_id(reply_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM intro_replies WHERE id = %s", (reply_id,))
    deleted = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return deleted