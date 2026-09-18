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
    init_pool,
    init_channels_table,
    create_channel,
    init_forum_messages_table,
    init_intro_threads_tables,
    init_message_reactions_table,
    get_donator_by_id,
    get_channel_history,
    save_channel_message,
    get_intro_threads,
    get_intro_thread_by_donator,
    upsert_intro_thread,
    get_intro_thread_owner,
    delete_intro_thread_by_id,
    get_intro_replies,
    add_intro_reply,
    get_intro_reply_owner,
    update_intro_reply,
    delete_intro_reply_by_id,
    get_donators_by_ids,
    get_forum_message_sender,
    delete_forum_message_by_id,
    update_forum_message,
    get_reactions_for_messages,
    toggle_reaction
)

load_dotenv()

IS_PRODUCTION = (
    os.environ.get("FLASK_ENV") == "production"
    or os.environ.get("APP_ENV") == "production"
    or os.environ.get("NODE_ENV") == "production"
)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY')

app.config['SESSION_COOKIE_SAMESITE'] = 'None' if IS_PRODUCTION else 'Lax'
app.config['SESSION_COOKIE_SECURE'] = IS_PRODUCTION
if IS_PRODUCTION:
    app.config['SESSION_COOKIE_DOMAIN'] = '.roll4rights.duckdns.org'
else:
    app.config.pop('SESSION_COOKIE_DOMAIN', None)

ALLOWED_ORIGINS = os.environ.get(
    'ALLOWED_ORIGINS',
    'http://localhost:5173,http://127.0.0.1:5173'
).split(',')

CORS(
    app,
    resources={r"/api/*": {"origins": ALLOWED_ORIGINS}},
    supports_credentials=True,
    allow_headers=["Content-Type", "Authorization"],
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"]
)

@app.after_request
def after_request(response):
    origin = request.headers.get("Origin")
    if origin in ALLOWED_ORIGINS:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
    return response

socketio = SocketIO(app, cors_allowed_origins=ALLOWED_ORIGINS, async_mode='gevent')

DEFAULT_CHANNELS = [
    ("welcome", "Welcome", "static"),
    ("pinned-information", "Introduce Yourself", "threads"),
    ("general-chat", "General Chat", "live"),
    ("donation-talk", "Donation Talk", "live"),
    ("dice-chat", "Dice Chat", "live"),
]


def initialize_database():
    init_pool()
    init_channels_table()
    init_forum_messages_table()
    init_intro_threads_tables()
    init_message_reactions_table()

    for slug, name, channel_type in DEFAULT_CHANNELS:
        create_channel(slug, name, channel_type)

    app.logger.info("Chat database initialized")


initialize_database()

login_manager = LoginManager()
login_manager.init_app(app)


class Donator(UserMixin):
    def __init__(self, id, name, email, is_admin=False):
        self.id = id
        self.name = name
        self.email = email
        self.is_admin = is_admin

    def get_id(self):
        return str(self.id)


@login_manager.user_loader
def load_user(donator_id):
    row = get_donator_by_id(donator_id)
    if not row:
        return None
    return Donator(row['id'], row['name'], row['email'], row['is_admin'])


@login_manager.unauthorized_handler
def unauthorized():
    return jsonify({'error': 'Login required'}), 401


def _picture_path(donator):
    if donator and donator.get('profile_picture'):
        return f"/profile-pictures/{donator['profile_picture']}"
    return None


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

REACTION_EMOJIS = ['❤️', '👍', '😂', '😮', '😢', '🎉']


def _format_message(row, donators, reactions_by_message=None):
    """Shapes a forum_messages row (from save_channel_message /
    get_channel_history) into the payload ChannelChat.vue expects:
    senderID (capital ID), timestamp, a replyTo PREVIEW OBJECT (not just an
    id), and reactions as [{emoji, donatorIds}, ...].
    """
    reactions_by_message = reactions_by_message or {}
    sender = donators.get(str(row['sender_id']))

    reply_to = None
    if row['reply_to_id']:
        reply_to = {
            'id': row['reply_to_id'],
            'senderName': row.get('reply_sender_name'),
            'message': row.get('reply_message')
        }

    return {
        'id': row['id'],
        'channel': row['channel_slug'],
        'message': row['message'],
        'senderID': row['sender_id'],
        'senderName': (sender.get('name') if sender else None) or row['sender_name'],
        'senderPicture': _picture_path(sender),
        'replyTo': reply_to,
        'timestamp': row['created_at'].isoformat(),
        'editedAt': row['edited_at'].isoformat() if row['edited_at'] else None,
        'reactions': reactions_by_message.get(row['id'], [])
    }


@socketio.on('connect')
def handle_connect():
    if not current_user.is_authenticated:
        app.logger.warning("Rejected unauthenticated Socket.IO connection")
        return False


def _authenticated_socket_user():
    if not current_user.is_authenticated:
        app.logger.warning(
            "Rejected unauthenticated Socket.IO event: %s",
            request.event.get("message") if request.event else "unknown"
        )
        return None

    return current_user


@socketio.on('join_channel')
def handle_join_channel(data):
    user = _authenticated_socket_user()
    if user is None:
        return

    channel = (data or {}).get('channel')
    if not channel:
        return



    join_room(channel)
    history = get_channel_history(channel)

    sender_ids = {str(row['sender_id']) for row in history}
    donators = get_donators_by_ids(sender_ids)
    reactions_by_message = get_reactions_for_messages(
        [row['id'] for row in history]
    )

    emit('channel_history', {
        'channel': channel,
        'messages': [
            _format_message(row, donators, reactions_by_message)
            for row in history
        ]
    })


@socketio.on('leave_channel')
def handle_leave_channel(data):
    user = _authenticated_socket_user()
    if user is None:
        return

    channel = (data or {}).get('channel')
    if channel:
        leave_room(channel)


@socketio.on('send_channel_message')
def handle_send_channel_message(data):
    user = _authenticated_socket_user()
    if user is None:
        return

    channel = data.get('channel')
    message = (data.get('message') or '').strip()
    reply_to_id = data.get('replyTo')

    if not channel or not message:
        return


    saved = save_channel_message(
        channel,
        str(user.id),
        user.name,
        message,
        reply_to_id=reply_to_id
    )

    donator = get_donator_by_id(user.id)
    donators = {str(user.id): donator} if donator else {}

    emit('channel_message', _format_message(saved, donators), room=channel)


@socketio.on('edit_message')
def handle_edit_message(data):
    user = _authenticated_socket_user()
    if user is None:
        return

    message_id = data.get('messageId')
    channel = data.get('channel')
    new_text = (data.get('message') or '').strip()

    if not message_id or not channel or not new_text:
        return

    sender_id = get_forum_message_sender(message_id)
    if sender_id is None or sender_id != str(current_user.id):
        return

    updated = update_forum_message(message_id, new_text)
    if not updated:
        return

    emit('message_edited', {
        'id': updated['id'],
        'channel': channel,
        'message': updated['message'],
        'editedAt': updated['edited_at'].isoformat()
    }, room=channel)


@socketio.on('delete_message')
def handle_delete_message(data):
    user = _authenticated_socket_user()
    if user is None:
        return

    message_id = data.get('messageId')
    channel = data.get('channel')
    if not message_id or not channel:
        return

    sender_id = get_forum_message_sender(message_id)
    if sender_id is None:
        return
    if sender_id != str(current_user.id) and not current_user.is_admin:
        return  # not their message, and not an admin — silently ignored

    delete_forum_message_by_id(message_id)
    emit('message_deleted', {'id': message_id, 'channel': channel}, room=channel)


@socketio.on('toggle_reaction')
def handle_toggle_reaction(data):
    user = _authenticated_socket_user()
    if user is None:
        return

    message_id = data.get('messageId')
    channel = data.get('channel')
    emoji = data.get('emoji')

    if not message_id or not channel or emoji not in REACTION_EMOJIS:
        return

    reactions = toggle_reaction(message_id, str(current_user.id), emoji)
    emit('reactions_updated', {'id': message_id, 'channel': channel, 'reactions': reactions}, room=channel)


# ============= INTRO THREADS ("Introduce Yourself") =============

@app.route('/api/forum/intro-threads', methods=['GET'])
@login_required
def list_intro_threads():
    try:
        page = max(1, int(request.args.get('page', 1)))
        per_page = 10
        rows, total = get_intro_threads(page=page, per_page=per_page)

        donator_ids = {str(row['donator_id']) for row in rows}
        donators = get_donators_by_ids(donator_ids)

        return jsonify({
            'threads': [
                {
                    'id': row['id'], 'donatorId': row['donator_id'],
                    'author': donators.get(str(row['donator_id']), {}).get('name') or row['author_name'],
                    'authorPicture': _picture_path(donators.get(str(row['donator_id']))),
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
        donator = get_donator_by_id(current_user.id)
        return jsonify({
            'id': row['id'], 'donatorId': row['donator_id'],
            'author': (donator['name'] if donator else None) or row['author_name'],
            'authorPicture': _picture_path(donator),
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

        donator_ids = {str(row['donator_id']) for row in rows}
        donators = get_donators_by_ids(donator_ids)

        return jsonify({
            'replies': [
                {
                    'id': row['id'], 'threadId': row['thread_id'], 'donatorId': row['donator_id'],
                    'author': donators.get(str(row['donator_id']), {}).get('name') or row['author_name'],
                    'authorPicture': _picture_path(donators.get(str(row['donator_id']))),
                    'message': row['message'],
                    'createdAt': row['created_at'].isoformat(),
                    'editedAt': row['edited_at'].isoformat() if row['edited_at'] else None
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
            'createdAt': saved['created_at'].isoformat(),
            'editedAt': None
        }), 201
    except Exception as e:
        app.logger.error(f"Create intro reply error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/forum/intro-replies/<int:reply_id>', methods=['PATCH'])
@login_required
@csrf_protect
def edit_intro_reply(reply_id):
    try:
        owner_id = get_intro_reply_owner(reply_id)
        if owner_id is None or owner_id != current_user.id:
            return jsonify({'error': 'Not found'}), 404

        data = request.json or {}
        message = data.get('message', '').strip()
        if not message:
            return jsonify({'error': 'Message is required'}), 400

        updated = update_intro_reply(reply_id, message)
        return jsonify({
            'id': updated['id'], 'threadId': updated['thread_id'], 'donatorId': updated['donator_id'],
            'author': updated['author_name'], 'message': updated['message'],
            'createdAt': updated['created_at'].isoformat(),
            'editedAt': updated['edited_at'].isoformat() if updated['edited_at'] else None
        }), 200
    except Exception as e:
        app.logger.error(f"Edit intro reply error: {e}")
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


@app.route('/')
def index():
    return jsonify({'service': 'Roll4Rights Chat', 'status': 'running'})


if __name__ == '__main__':
    socketio.run(app, port=5001, host='0.0.0.0')