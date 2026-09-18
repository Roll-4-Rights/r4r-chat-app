import os
import psycopg2
import psycopg2.extras
from psycopg2 import pool as pg_pool

_pool = None


def init_pool():
    global _pool

    if _pool is not None:
        return

    _pool = pg_pool.ThreadedConnectionPool(
        minconn=2,
        maxconn=10,
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", 5432),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        cursor_factory=psycopg2.extras.RealDictCursor,
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
        try:
            self._real_conn.rollback()
        finally:
            _pool.putconn(self._real_conn)


def get_db_connection():
    if _pool is None:
        init_pool()

    return _PooledConnection(_pool.getconn())


# ============= SCHEMA =============
# NOTE ON TYPES: `sender_id` on forum_messages is stored as TEXT because
# chat_app.py always passes str(user.id) into save_channel_message() and
# compares it against str(current_user.id) when checking ownership.
# `donator_id` on intro_threads/intro_replies is stored as INTEGER because
# chat_app.py passes current_user.id (unstringified) into upsert_intro_thread
# / add_intro_reply, and compares ownership directly against current_user.id
# (e.g. `owner_id != current_user.id`) with no str() cast. Keeping these
# consistent with how chat_app.py actually uses them matters — mixing them
# up silently breaks every ownership check (edit/delete "my" content).

def init_channels_table():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS forum_channels (
                    slug TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    channel_type TEXT NOT NULL
                        CHECK (channel_type IN ('live', 'static', 'threads')),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def create_channel(slug, name, channel_type="live"):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO forum_channels (slug, name, channel_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (slug) DO UPDATE
                SET name = EXCLUDED.name,
                    channel_type = EXCLUDED.channel_type
            """, (slug, name, channel_type))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_forum_messages_table():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS forum_messages (
                    id SERIAL PRIMARY KEY,
                    channel_slug TEXT NOT NULL REFERENCES forum_channels(slug),
                    sender_id TEXT NOT NULL,
                    sender_name TEXT NOT NULL,
                    message TEXT NOT NULL,
                    reply_to_id INTEGER REFERENCES forum_messages(id) ON DELETE SET NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    edited_at TIMESTAMPTZ
                )
            """)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_intro_threads_tables():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS intro_threads (
                    id SERIAL PRIMARY KEY,
                    donator_id INTEGER NOT NULL UNIQUE,
                    author_name TEXT NOT NULL,
                    title TEXT NOT NULL,
                    body TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS intro_replies (
                    id SERIAL PRIMARY KEY,
                    thread_id INTEGER NOT NULL REFERENCES intro_threads(id) ON DELETE CASCADE,
                    donator_id INTEGER NOT NULL,
                    author_name TEXT NOT NULL,
                    message TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    edited_at TIMESTAMPTZ
                )
            """)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_message_reactions_table():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS message_reactions (
                    id SERIAL PRIMARY KEY,
                    message_id INTEGER NOT NULL REFERENCES forum_messages(id) ON DELETE CASCADE,
                    user_id TEXT NOT NULL,
                    reaction TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (message_id, user_id, reaction)
                )
            """)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ============= DONATORS (users) =============

def get_donator_by_id(donator_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE id = %s", (donator_id,))
            return cur.fetchone()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_donators_by_ids(donator_ids):
    """donator_ids: any iterable of ids (str or int). Returns a dict keyed
    by str(id) -> user row, since every call site looks these up via
    str(some_id)."""
    donator_ids = [str(i) for i in donator_ids]
    if not donator_ids:
        return {}

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM users WHERE id::text = ANY(%s)",
                (donator_ids,)
            )
            rows = cur.fetchall()
        return {str(row['id']): row for row in rows}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ============= LIVE CHANNEL MESSAGES =============

def get_channel_history(channel_slug, limit=50):
    """Returns the most recent `limit` messages, oldest first (so the
    frontend can render them top-to-bottom without re-sorting). Joins in
    the replied-to message's sender_name/message so a reply preview can be
    built without a second round trip."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT m.*,
                       p.sender_name AS reply_sender_name,
                       p.message AS reply_message
                FROM forum_messages m
                LEFT JOIN forum_messages p ON p.id = m.reply_to_id
                WHERE m.channel_slug = %s
                ORDER BY m.created_at DESC
                LIMIT %s
            """, (channel_slug, limit))
            rows = cur.fetchall()
        return list(reversed(rows))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def save_channel_message(channel_slug, sender_id, sender_name, message, reply_to_id=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO forum_messages (channel_slug, sender_id, sender_name, message, reply_to_id)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING *
            """, (channel_slug, sender_id, sender_name, message, reply_to_id))
            row = cur.fetchone()

            if row['reply_to_id']:
                cur.execute("""
                    SELECT sender_name, message FROM forum_messages WHERE id = %s
                """, (row['reply_to_id'],))
                parent = cur.fetchone()
                if parent:
                    row['reply_sender_name'] = parent['sender_name']
                    row['reply_message'] = parent['message']
        conn.commit()
        return row
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_forum_message_sender(message_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT sender_id FROM forum_messages WHERE id = %s", (message_id,))
            row = cur.fetchone()
        return row['sender_id'] if row else None
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_forum_message(message_id, new_text):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE forum_messages
                SET message = %s, edited_at = NOW()
                WHERE id = %s
                RETURNING *
            """, (new_text, message_id))
            row = cur.fetchone()
        conn.commit()
        return row
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def delete_forum_message_by_id(message_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM forum_messages WHERE id = %s", (message_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ============= REACTIONS =============
# Reaction shape is a list of {emoji, donatorIds} entries — one per emoji
# that has at least one reaction. Matches ChannelChat.vue, which reads
# r.emoji and r.donatorIds (e.g. r.donatorIds.includes(myId)).

def _rows_to_reaction_list(rows):
    by_emoji = {}
    for row in rows:
        by_emoji.setdefault(row['reaction'], []).append(row['user_id'])
    return [
        {'emoji': emoji, 'donatorIds': donator_ids}
        for emoji, donator_ids in by_emoji.items()
    ]


def get_reactions_for_messages(message_ids):
    message_ids = list(message_ids)
    if not message_ids:
        return {}

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT message_id, reaction, user_id
                FROM message_reactions
                WHERE message_id = ANY(%s)
            """, (message_ids,))
            rows = cur.fetchall()

        grouped = {}
        for row in rows:
            grouped.setdefault(row['message_id'], []).append(row)
        return {
            message_id: _rows_to_reaction_list(rows_for_message)
            for message_id, rows_for_message in grouped.items()
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def toggle_reaction(message_id, user_id, emoji):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM message_reactions
                WHERE message_id = %s AND user_id = %s AND reaction = %s
            """, (message_id, user_id, emoji))

            if cur.rowcount == 0:
                cur.execute("""
                    INSERT INTO message_reactions (message_id, user_id, reaction)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (message_id, user_id, reaction) DO NOTHING
                """, (message_id, user_id, emoji))

            cur.execute("""
                SELECT reaction, user_id FROM message_reactions WHERE message_id = %s
            """, (message_id,))
            rows = cur.fetchall()

        conn.commit()
        return _rows_to_reaction_list(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ============= INTRO THREADS ("Introduce Yourself") =============

def get_intro_threads(page=1, per_page=10):
    conn = get_db_connection()
    try:
        offset = (page - 1) * per_page
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS total FROM intro_threads")
            total = cur.fetchone()['total']

            cur.execute("""
                SELECT t.*, COUNT(r.id) AS reply_count
                FROM intro_threads t
                LEFT JOIN intro_replies r ON r.thread_id = t.id
                GROUP BY t.id
                ORDER BY t.created_at DESC
                LIMIT %s OFFSET %s
            """, (per_page, offset))
            rows = cur.fetchall()
        return rows, total
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_intro_thread_by_donator(donator_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM intro_threads WHERE donator_id = %s", (donator_id,))
            return cur.fetchone()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_intro_thread(donator_id, author_name, title, body):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO intro_threads (donator_id, author_name, title, body)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (donator_id) DO UPDATE
                SET author_name = EXCLUDED.author_name,
                    title = EXCLUDED.title,
                    body = EXCLUDED.body
                RETURNING *
            """, (donator_id, author_name, title, body))
            row = cur.fetchone()
        conn.commit()
        return row
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_intro_thread_owner(thread_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT donator_id FROM intro_threads WHERE id = %s", (thread_id,))
            row = cur.fetchone()
        return row['donator_id'] if row else None
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def delete_intro_thread_by_id(thread_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM intro_threads WHERE id = %s", (thread_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_intro_replies(thread_id, page=1, per_page=10):
    conn = get_db_connection()
    try:
        offset = (page - 1) * per_page
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS total FROM intro_replies WHERE thread_id = %s",
                (thread_id,)
            )
            total = cur.fetchone()['total']

            cur.execute("""
                SELECT * FROM intro_replies
                WHERE thread_id = %s
                ORDER BY created_at ASC
                LIMIT %s OFFSET %s
            """, (thread_id, per_page, offset))
            rows = cur.fetchall()
        return rows, total
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def add_intro_reply(thread_id, donator_id, author_name, message):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO intro_replies (thread_id, donator_id, author_name, message)
                VALUES (%s, %s, %s, %s)
                RETURNING *
            """, (thread_id, donator_id, author_name, message))
            row = cur.fetchone()
        conn.commit()
        return row
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_intro_reply_owner(reply_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT donator_id FROM intro_replies WHERE id = %s", (reply_id,))
            row = cur.fetchone()
        return row['donator_id'] if row else None
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_intro_reply(reply_id, new_message):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE intro_replies
                SET message = %s, edited_at = NOW()
                WHERE id = %s
                RETURNING *
            """, (new_message, reply_id))
            row = cur.fetchone()
        conn.commit()
        return row
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def delete_intro_reply_by_id(reply_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM intro_replies WHERE id = %s", (reply_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()