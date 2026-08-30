# chat_app.py — dedicated real-time + forum service. Deployed separately from
# the main API so Socket.IO's single-worker requirement only affects this
# process, not the whole site.

from gevent import monkey
monkey.patch_all()

from psycogreen.gevent import patch_psycopg
patch_psycopg()

import math
import os
from functools import wraps
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_login import LoginManager, UserMixin, login_required, current_user
from flask_socketio import SocketIO, emit, join_room, leave_room
from dotenv import load_dotenv

from db import (
    get_db_connection, get_donator_by_id, init_forum_messages_table, get_channel_history, save_channel_message,
    init_intro_threads_tables, get_intro_threads, get_intro_thread_by_donator, upsert_intro_thread,
    get_intro_thread_owner, delete_intro_thread_by_id, get_intro_replies, add_intro_reply,
    get_intro_reply_owner, delete_intro_reply_by_id
)

load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY')  # must match the main API exactly

app.config['SESSION_COOKIE_SAMESITE'] = 'None'
app.config['SESSION_COOKIE_SECURE'] = True
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_DOMAIN'] = '.roll4rights.duckdns.org'

ALLOWED_ORIGINS = os.environ.get('ALLOWED_ORIGINS', 'http://localhost:5173').split(',')

CORS(app, supports_credentials=True, origins=ALLOWED_ORIGINS)
socketio = SocketIO(app, cors_allowed_origins=ALLOWED_ORIGINS, async_mode='gevent')

init_forum_messages_table()
init_intro_threads_tables()

login_manager = LoginManager()
login_manager.init_app(app)


class Donator(UserMixin):
    def __init__(self, id, name, email):
        self.id = id
        self.name = name
        self.email = email

    def get_id(self):
        return str(self.id)


@login_manager.user_loader
def load_user(donator_id):
    row = get_donator_by_id(donator_id)
    if not row:
        return None
    return Donator(row['id'], row['name'], row['email'])


@login_manager.unauthorized_handler
def unauthorized():
    return jsonify({'error': 'Login required'}), 401


def csrf_protect(f):
    """Same Origin-check as the main API — duplicated here on purpose,
    since this is a separate, independently deployed service."""
    @wraps(f)
    def decorated(*args, **kwargs):
        origin = request.headers.get('Origin', '')
        if origin not in ALLOWED_ORIGINS:
            app.logger.warning(f"Blocked request with untrusted Origin: {origin!r}")
            return jsonify({'error': 'Untrusted origin'}), 403
        return f(*args, **kwargs)
    return decorated


# ============= LIVE CHAT (SOCKET.IO) =============

@socketio.on('connect')
def handle_connect():
    if not current_user.is_authenticated:
        app.logger.warning("Rejected unauthenticated Socket.IO connection")
        return False


@socketio.on('join_channel')
def handle_join_channel(data):
    channel = data.get('channel')
    if not channel:
        return
    join_room(channel)
    history = get_channel_history(channel)
    emit('channel_history', {
        'channel': channel,
        'messages': [
            {
                'id': row['id'],
                'channel': row['channel'],
                'senderID': row['sender_id'],
                'senderName': row['sender_name'],
                'message': row['message'],
                'timestamp': row['created_at'].isoformat()
            }
            for row in history
        ]
    })


@socketio.on('leave_channel')
def handle_leave_channel(data):
    channel = data.get('channel')
    if channel:
        leave_room(channel)


@socketio.on('send_channel_message')
def handle_send_channel_message(data):
    channel = data.get('channel')
    message = (data.get('message') or '').strip()

    if not channel or not message:
        return

    saved = save_channel_message(channel, str(current_user.id), current_user.name, message)

    emit('channel_message', {
        'id': saved['id'],
        'channel': saved['channel'],
        'senderID': saved['sender_id'],
        'senderName': saved['sender_name'],
        'message': saved['message'],
        'timestamp': saved['created_at'].isoformat()
    }, room=channel)


# ============= INTRO THREADS ("Introduce Yourself") =============

@app.route('/api/forum/intro-threads', methods=['GET'])
@login_required
def list_intro_threads():
    try:
        page = max(1, int(request.args.get('page', 1)))
        per_page = 10
        rows, total = get_intro_threads(page=page, per_page=per_page)
        return jsonify({
            'threads': [
                {
                    'id': row['id'], 'donatorId': row['donator_id'], 'author': row['author_name'],
                    'title': row['title'], 'body': row['body'],
                    'createdAt': row['created_at'].isoformat(), 'replyCount': row['reply_count']
                }
                for row in rows
            ],
            'page': page,
            'totalPages': max(1, math.ceil(total / per_page))
        }), 200
    except Exception as e:
        app.logger.error(f"List intro threads error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/forum/intro-threads/mine', methods=['GET'])
@login_required
def get_my_intro_thread():
    try:
        row = get_intro_thread_by_donator(current_user.id)
        if not row:
            return jsonify(None), 200
        return jsonify({
            'id': row['id'], 'donatorId': row['donator_id'], 'author': row['author_name'],
            'title': row['title'], 'body': row['body'], 'createdAt': row['created_at'].isoformat()
        }), 200
    except Exception as e:
        app.logger.error(f"Get my intro thread error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/forum/intro-threads', methods=['POST'])
@login_required
@csrf_protect
def save_intro_thread():
    try:
        data = request.json or {}
        title = data.get('title', '').strip()
        body = data.get('body', '').strip()
        if not title or not body:
            return jsonify({'error': 'Title and body are required'}), 400

        saved = upsert_intro_thread(current_user.id, current_user.name, title, body)
        return jsonify({
            'id': saved['id'], 'donatorId': saved['donator_id'], 'author': saved['author_name'],
            'title': saved['title'], 'body': saved['body'], 'createdAt': saved['created_at'].isoformat()
        }), 200
    except Exception as e:
        app.logger.error(f"Save intro thread error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/forum/intro-threads/<int:thread_id>', methods=['DELETE'])
@login_required
@csrf_protect
def remove_intro_thread(thread_id):
    try:
        owner_id = get_intro_thread_owner(thread_id)
        if owner_id is None or owner_id != current_user.id:
            return jsonify({'error': 'Not found'}), 404
        delete_intro_thread_by_id(thread_id)
        return jsonify({'message': 'Deleted'}), 200
    except Exception as e:
        app.logger.error(f"Delete intro thread error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/forum/intro-threads/<int:thread_id>/replies', methods=['GET'])
@login_required
def list_intro_replies(thread_id):
    try:
        page = max(1, int(request.args.get('page', 1)))
        per_page = 10
        rows, total = get_intro_replies(thread_id, page=page, per_page=per_page)
        return jsonify({
            'replies': [
                {
                    'id': row['id'], 'threadId': row['thread_id'], 'donatorId': row['donator_id'],
                    'author': row['author_name'], 'message': row['message'],
                    'createdAt': row['created_at'].isoformat()
                }
                for row in rows
            ],
            'page': page,
            'totalPages': max(1, math.ceil(total / per_page))
        }), 200
    except Exception as e:
        app.logger.error(f"List intro replies error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/forum/intro-threads/<int:thread_id>/replies', methods=['POST'])
@login_required
@csrf_protect
def create_intro_reply(thread_id):
    try:
        data = request.json or {}
        message = data.get('message', '').strip()
        if not message:
            return jsonify({'error': 'Message is required'}), 400
        if get_intro_thread_owner(thread_id) is None:
            return jsonify({'error': 'Thread not found'}), 404

        saved = add_intro_reply(thread_id, current_user.id, current_user.name, message)
        return jsonify({
            'id': saved['id'], 'threadId': saved['thread_id'], 'donatorId': saved['donator_id'],
            'author': saved['author_name'], 'message': saved['message'],
            'createdAt': saved['created_at'].isoformat()
        }), 201
    except Exception as e:
        app.logger.error(f"Create intro reply error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/forum/intro-replies/<int:reply_id>', methods=['DELETE'])
@login_required
@csrf_protect
def remove_intro_reply(reply_id):
    try:
        owner_id = get_intro_reply_owner(reply_id)
        if owner_id is None or owner_id != current_user.id:
            return jsonify({'error': 'Not found'}), 404
        delete_intro_reply_by_id(reply_id)
        return jsonify({'message': 'Deleted'}), 200
    except Exception as e:
        app.logger.error(f"Delete intro reply error: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    socketio.run(app, port=5001, host='0.0.0.0')