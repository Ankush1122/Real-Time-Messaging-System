import socket
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget

from chatapp_core.protocol import recv_message, send_message

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 12345

APP_STYLESHEET = """
QWidget {
    background: #f5f7fb;
    color: #1f2937;
    font-family: 'Inter', 'Segoe UI', Arial, sans-serif;
    font-size: 13px;
}
QFrame#LoginCard, QFrame#SidebarCard, QFrame#ChatCard, QFrame#ComposerCard {
    background: white;
    border: 1px solid #e5e7eb;
    border-radius: 16px;
}
QLineEdit, QPlainTextEdit {
    background: #ffffff;
    border: 1px solid #d1d5db;
    border-radius: 12px;
    padding: 10px 12px;
}
QLineEdit:focus, QPlainTextEdit:focus {
    border: 1px solid #2563eb;
}
QPushButton {
    background: #2563eb;
    color: white;
    border: none;
    border-radius: 12px;
    padding: 10px 16px;
    font-weight: 600;
}
QPushButton:hover {
    background: #1d4ed8;
}
QPushButton:disabled {
    background: #93c5fd;
    color: #eff6ff;
}
QPushButton#SecondaryButton {
    background: #eef2ff;
    color: #1e40af;
}
QPushButton#SecondaryButton:hover {
    background: #dbeafe;
}
QToolButton {
    border: none;
    color: #4b5563;
    font-weight: 600;
}
QListWidget {
    background: transparent;
    border: none;
    outline: none;
}
QListWidget::item {
    border: none;
}
QScrollBar:vertical {
    background: transparent;
    width: 10px;
    margin: 4px;
}
QScrollBar::handle:vertical {
    background: #cbd5e1;
    border-radius: 5px;
    min-height: 30px;
}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    height: 0px;
}
QStatusBar {
    background: #ffffff;
    border-top: 1px solid #e5e7eb;
}
"""


@dataclass
class User:
    user_id: str
    username: str


@dataclass
class ChatMessage:
    author_id: str
    author_name: str
    recipient_id: str
    body: str
    server_ts: float
    delivery_status: str = 'pending'
    client_message_id: Optional[str] = None
    message_id: Optional[int] = None
    chat_id: Optional[int] = None
    inserted_at: Optional[str] = None


@dataclass
class ChatSummary:
    chat_id: int
    peer_id: str
    peer_username: str
    last_message: str = ''
    last_message_at: Optional[str] = None
    last_message_id: Optional[int] = None


class Signals(QWidget):
    connected = pyqtSignal(dict)
    login_failed = pyqtSignal(str)
    connection_failed = pyqtSignal(str)
    chat_started = pyqtSignal(dict)
    chat_start_failed = pyqtSignal(dict)
    chat_list = pyqtSignal(list)
    history_loaded = pyqtSignal(dict)
    message_received = pyqtSignal(dict)
    delivery_updated = pyqtSignal(dict)
    server_disconnected = pyqtSignal(str)


class ChatState:
    def __init__(self) -> None:
        self.self_user: Optional[User] = None
        self.peers: Dict[str, User] = {}
        self.chat_summaries_by_peer: Dict[str, ChatSummary] = {}
        self.peer_by_chat_id: Dict[int, str] = {}
        self.messages_by_peer: Dict[str, List[ChatMessage]] = {}
        self.pending_messages: Dict[str, ChatMessage] = {}

    def set_self(self, user_id: str, username: str) -> None:
        self.self_user = User(user_id=str(user_id), username=username)

    def add_peer(self, user_id: str, username: Optional[str] = None) -> User:
        user_id = str(user_id).strip()
        username = str(username or user_id).strip()
        user = User(user_id=user_id, username=username)
        self.peers[user_id] = user
        self.messages_by_peer.setdefault(user_id, [])
        return user

    def upsert_chat_summary(self, payload: dict) -> Optional[ChatSummary]:
        peer = payload.get('peer') or {}
        peer_id = str(peer.get('user_id') or '').strip()
        if not peer_id:
            return None
        peer_username = str(peer.get('username') or peer_id).strip()
        self.add_peer(peer_id, peer_username)
        chat_id = int(payload.get('chat_id') or 0)
        summary = self.chat_summaries_by_peer.get(peer_id)
        if summary is None:
            summary = ChatSummary(chat_id=chat_id, peer_id=peer_id, peer_username=peer_username)
            self.chat_summaries_by_peer[peer_id] = summary
        else:
            summary.chat_id = chat_id or summary.chat_id
            summary.peer_username = peer_username
        if chat_id:
            self.peer_by_chat_id[chat_id] = peer_id
        if payload.get('last_message') is not None:
            summary.last_message = str(payload.get('last_message') or '')
        if payload.get('last_message_at') is not None:
            summary.last_message_at = payload.get('last_message_at')
        if payload.get('last_message_id') is not None:
            summary.last_message_id = payload.get('last_message_id')
        return summary

    def load_chat_list(self, chats: List[dict]) -> None:
        for chat in chats:
            self.upsert_chat_summary(chat)

    def get_chat_summary(self, peer_id: str) -> Optional[ChatSummary]:
        return self.chat_summaries_by_peer.get(str(peer_id))

    def sorted_chat_summaries(self) -> List[ChatSummary]:
        return sorted(
            self.chat_summaries_by_peer.values(),
            key=lambda c: (
                _sortable_timestamp(c.last_message_at),
                c.chat_id,
            ),
            reverse=True,
        )

    def replace_history(self, chat_id: int, messages: List[dict]) -> Optional[str]:
        peer_id = self.peer_by_chat_id.get(chat_id)
        if not peer_id:
            return None
        rendered: List[ChatMessage] = []
        for payload in messages:
            sender_id = str(payload.get('sender_id') or '')
            sender_username = str(payload.get('sender_username') or sender_id)
            is_self = self.self_user is not None and sender_id == self.self_user.user_id
            rendered.append(
                ChatMessage(
                    author_id=sender_id,
                    author_name='You' if is_self else sender_username,
                    recipient_id=peer_id if is_self else (self.self_user.user_id if self.self_user else ''),
                    body=str(payload.get('message') or ''),
                    server_ts=_to_epoch(payload.get('inserted_at')),
                    delivery_status='delivered_to_client' if is_self else 'delivered_to_client',
                    client_message_id=payload.get('client_message_id'),
                    message_id=payload.get('message_id'),
                    chat_id=payload.get('chat_id') or chat_id,
                    inserted_at=payload.get('inserted_at'),
                )
            )
        self.messages_by_peer[peer_id] = rendered
        if rendered:
            last = rendered[-1]
            self._update_summary_from_message(peer_id, last)
        return peer_id

    def add_outbound_message(self, peer_id: str, body: str) -> ChatMessage:
        if not self.self_user:
            raise RuntimeError('Self user missing')
        summary = self.chat_summaries_by_peer.get(peer_id)
        message = ChatMessage(
            author_id=self.self_user.user_id,
            author_name=self.self_user.username,
            recipient_id=peer_id,
            body=body,
            server_ts=time.time(),
            delivery_status='pending',
            client_message_id=str(uuid.uuid4()),
            chat_id=summary.chat_id if summary else None,
        )
        self.messages_by_peer.setdefault(peer_id, []).append(message)
        self.pending_messages[message.client_message_id] = message
        self._ensure_summary_for_peer(peer_id)
        self._update_summary_from_message(peer_id, message)
        return message

    def add_inbound_message(self, payload: dict) -> str:
        peer_id = str(payload.get('from_user_id') or '').strip()
        peer_name = str(payload.get('from_username') or peer_id).strip()
        self.add_peer(peer_id, peer_name)
        summary = self._ensure_summary_for_peer(peer_id)
        chat_id = int(payload.get('chat_id') or 0)
        if chat_id:
            summary.chat_id = chat_id
            self.peer_by_chat_id[chat_id] = peer_id
        message = ChatMessage(
            author_id=peer_id,
            author_name=peer_name,
            recipient_id=str(payload.get('to_user_id') or ''),
            body=str(payload.get('message') or ''),
            server_ts=_to_epoch(payload.get('inserted_at')) or float(payload.get('server_ts') or time.time()),
            delivery_status='delivered_to_client',
            client_message_id=payload.get('client_message_id'),
            message_id=payload.get('message_id'),
            chat_id=payload.get('chat_id'),
            inserted_at=payload.get('inserted_at'),
        )
        self.messages_by_peer.setdefault(peer_id, []).append(message)
        self._update_summary_from_message(peer_id, message)
        return peer_id

    def update_delivery(self, payload: dict) -> Optional[tuple[ChatMessage, str]]:
        client_message_id = payload.get('client_message_id')
        if not client_message_id:
            return None
        message = self.pending_messages.get(client_message_id)
        if message is None:
            return None
        status = str(payload.get('status') or 'unknown')
        message.delivery_status = status
        if payload.get('message_id'):
            message.message_id = payload.get('message_id')
        if payload.get('chat_id'):
            message.chat_id = payload.get('chat_id')
            self.peer_by_chat_id[int(payload['chat_id'])] = message.recipient_id
            summary = self._ensure_summary_for_peer(message.recipient_id)
            summary.chat_id = int(payload['chat_id'])
        if status in {'delivered_to_client', 'failed'}:
            self.pending_messages.pop(client_message_id, None)
        self._update_summary_from_message(message.recipient_id, message)
        return message, status

    def _ensure_summary_for_peer(self, peer_id: str) -> ChatSummary:
        peer = self.peers.get(peer_id) or self.add_peer(peer_id, peer_id)
        summary = self.chat_summaries_by_peer.get(peer_id)
        if summary is None:
            summary = ChatSummary(chat_id=0, peer_id=peer_id, peer_username=peer.username)
            self.chat_summaries_by_peer[peer_id] = summary
        return summary

    def _update_summary_from_message(self, peer_id: str, message: ChatMessage) -> None:
        summary = self._ensure_summary_for_peer(peer_id)
        summary.last_message = message.body
        summary.last_message_at = message.inserted_at or _to_iso(message.server_ts)
        if message.message_id:
            summary.last_message_id = message.message_id
        if message.chat_id:
            summary.chat_id = int(message.chat_id)
            self.peer_by_chat_id[int(message.chat_id)] = peer_id


class ChatClientThread(threading.Thread):
    def __init__(self, host: str, port: int, username: str, password: str, signals: Signals):
        super().__init__(daemon=True)
        self.host = host or DEFAULT_HOST
        self.port = int(port or DEFAULT_PORT)
        self.username = username.strip()
        self.user_id = self.username
        self.password = password
        self.signals = signals
        self.socket: Optional[socket.socket] = None
        self.running = True
        self.send_lock = threading.Lock()

    def run(self) -> None:
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            send_message(self.socket, {
                'type': 'login',
                'username': self.username,
                'user_id': self.user_id,
                'password': self.password,
                'create_if_missing': True,
            })
            while self.running:
                payload = recv_message(self.socket)
                self._dispatch(payload)
        except Exception as exc:
            if self.running:
                self.signals.connection_failed.emit(str(exc))
        finally:
            self.running = False
            if self.socket is not None:
                try:
                    self.socket.close()
                except Exception:
                    pass
            self.signals.server_disconnected.emit('Disconnected from server')

    def start_chat(self, peer_id: str) -> None:
        self._send({'type': 'start_chat', 'to_user_id': peer_id})

    def fetch_chats(self) -> None:
        self._send({'type': 'fetch_chats', 'limit': 100})

    def fetch_history(self, chat_id: Optional[int] = None, peer_id: Optional[str] = None, limit: int = 50) -> None:
        payload = {'type': 'fetch_history', 'limit': limit}
        if chat_id:
            payload['chat_id'] = chat_id
        if peer_id:
            payload['peer_id'] = peer_id
        self._send(payload)

    def send_chat(self, recipient_id: str, message: str, client_message_id: str, chat_id: Optional[int]) -> None:
        payload = {
            'type': 'send_message',
            'to_user_id': recipient_id,
            'message': message,
            'client_message_id': client_message_id,
        }
        if chat_id:
            payload['chat_id'] = chat_id
        self._send(payload)

    def close(self) -> None:
        self.running = False
        if self.socket is not None:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self.socket.close()
            except Exception:
                pass

    def _dispatch(self, payload: dict) -> None:
        message_type = payload.get('type')
        if message_type == 'login_ok':
            self.signals.connected.emit(payload)
        elif message_type == 'login_failed':
            self.signals.login_failed.emit(payload.get('message', 'Login failed'))
        elif message_type == 'chat_started':
            self.signals.chat_started.emit(payload)
        elif message_type == 'chat_start_failed':
            self.signals.chat_start_failed.emit(payload)
        elif message_type == 'chat_list':
            self.signals.chat_list.emit(payload.get('chats') or [])
        elif message_type == 'history':
            self.signals.history_loaded.emit(payload)
        elif message_type == 'chat_message':
            if payload.get('message_id') and payload.get('chat_id'):
                self._send({
                    'type': 'message_received_ack',
                    'message_id': payload.get('message_id'),
                    'chat_id': payload.get('chat_id'),
                    'from_user_id': payload.get('from_user_id'),
                    'client_message_id': payload.get('client_message_id'),
                })
            self.signals.message_received.emit(payload)
        elif message_type == 'delivery_status':
            self.signals.delivery_updated.emit(payload)
        elif message_type == 'pong':
            pass
        elif message_type == 'error':
            self.signals.connection_failed.emit(payload.get('message', 'Unknown server error'))

    def _send(self, payload: dict) -> None:
        if not self.socket:
            raise RuntimeError('Socket is not connected')
        with self.send_lock:
            send_message(self.socket, payload)


class LoginPage(QtWidgets.QWidget):
    login_requested = pyqtSignal(str, int, str, str)

    def __init__(self) -> None:
        super().__init__()
        self._build_ui()

    def _build_ui(self) -> None:
        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(48, 36, 48, 36)
        root.setSpacing(0)
        root.addStretch(1)

        card = QtWidgets.QFrame(objectName='LoginCard')
        card.setMaximumWidth(460)
        card_layout = QtWidgets.QVBoxLayout(card)
        card_layout.setContentsMargins(28, 28, 28, 28)
        card_layout.setSpacing(14)

        badge = QtWidgets.QLabel('Real-Time Messaging System')
        badge.setStyleSheet('color: #2563eb; font-size: 12px; font-weight: 700;')
        title = QtWidgets.QLabel('Welcome back')
        title.setStyleSheet('font-size: 28px; font-weight: 700; color: #111827;')
        subtitle = QtWidgets.QLabel('Sign in with your user ID and password. Server settings are already filled in for local development.')
        subtitle.setWordWrap(True)
        subtitle.setStyleSheet('color: #6b7280; font-size: 13px;')

        self.username_edit = QtWidgets.QLineEdit()
        self.username_edit.setPlaceholderText('User ID, for example 1')
        self.password_edit = QtWidgets.QLineEdit()
        self.password_edit.setPlaceholderText('Password')
        self.password_edit.setEchoMode(QtWidgets.QLineEdit.Password)

        self.advanced_toggle = QtWidgets.QToolButton(text='Server settings')
        self.advanced_toggle.setCheckable(True)
        self.advanced_toggle.setChecked(False)
        self.advanced_toggle.setToolButtonStyle(QtCore.Qt.ToolButtonTextBesideIcon)
        self.advanced_toggle.setArrowType(QtCore.Qt.RightArrow)
        self.advanced_toggle.toggled.connect(self._toggle_advanced)

        self.advanced_container = QtWidgets.QWidget()
        self.advanced_container.setVisible(False)
        advanced_layout = QtWidgets.QFormLayout(self.advanced_container)
        advanced_layout.setContentsMargins(0, 0, 0, 0)
        advanced_layout.setSpacing(10)
        self.host_edit = QtWidgets.QLineEdit(DEFAULT_HOST)
        self.port_edit = QtWidgets.QLineEdit(str(DEFAULT_PORT))
        advanced_layout.addRow('Host', self.host_edit)
        advanced_layout.addRow('Port', self.port_edit)

        self.login_button = QtWidgets.QPushButton('Connect')
        self.login_button.clicked.connect(self._submit)
        self.password_edit.returnPressed.connect(self._submit)

        self.feedback_label = QtWidgets.QLabel('')
        self.feedback_label.setStyleSheet('color: #dc2626; font-size: 12px;')
        self.feedback_label.setWordWrap(True)
        self.feedback_label.hide()

        sample_label = QtWidgets.QLabel('Tip: for local testing you can sign in as user 1 with password loadtest.')
        sample_label.setWordWrap(True)
        sample_label.setStyleSheet('background: #eff6ff; color: #1d4ed8; border-radius: 12px; padding: 10px 12px;')

        card_layout.addWidget(badge)
        card_layout.addWidget(title)
        card_layout.addWidget(subtitle)
        card_layout.addSpacing(6)
        card_layout.addWidget(QtWidgets.QLabel('User ID'))
        card_layout.addWidget(self.username_edit)
        card_layout.addWidget(QtWidgets.QLabel('Password'))
        card_layout.addWidget(self.password_edit)
        card_layout.addWidget(self.advanced_toggle)
        card_layout.addWidget(self.advanced_container)
        card_layout.addWidget(sample_label)
        card_layout.addWidget(self.feedback_label)
        card_layout.addWidget(self.login_button)

        row = QtWidgets.QHBoxLayout()
        row.addStretch(1)
        row.addWidget(card)
        row.addStretch(1)
        root.addLayout(row)
        root.addStretch(1)

    def _toggle_advanced(self, checked: bool) -> None:
        self.advanced_toggle.setArrowType(QtCore.Qt.DownArrow if checked else QtCore.Qt.RightArrow)
        self.advanced_container.setVisible(checked)

    def _submit(self) -> None:
        username = self.username_edit.text().strip()
        password = self.password_edit.text()
        host = self.host_edit.text().strip() or DEFAULT_HOST
        port_text = self.port_edit.text().strip() or str(DEFAULT_PORT)
        if not username:
            self.set_feedback('Enter a user ID before connecting.')
            return
        if not password:
            self.set_feedback('Enter a password before connecting.')
            return
        try:
            port = int(port_text)
        except ValueError:
            self.set_feedback('Port must be a number.')
            return
        self.set_feedback('')
        self.login_requested.emit(host, port, username, password)

    def set_feedback(self, text: str) -> None:
        if text:
            self.feedback_label.setText(text)
            self.feedback_label.show()
        else:
            self.feedback_label.hide()
            self.feedback_label.setText('')


class ChatPreviewWidget(QtWidgets.QFrame):
    def __init__(self, summary: ChatSummary, selected: bool = False):
        super().__init__()
        self.summary = summary
        self._build_ui(selected)

    def _build_ui(self, selected: bool) -> None:
        bg = '#eff6ff' if selected else 'transparent'
        border = '#bfdbfe' if selected else 'transparent'
        self.setStyleSheet(f'QFrame {{ background: {bg}; border: 1px solid {border}; border-radius: 12px; }}')
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(4)

        top_row = QtWidgets.QHBoxLayout()
        name = QtWidgets.QLabel(self.summary.peer_username)
        name.setStyleSheet('font-weight: 700; color: #111827;')
        time_label = QtWidgets.QLabel(humanize_timestamp_short(self.summary.last_message_at))
        time_label.setStyleSheet('color: #6b7280; font-size: 11px;')
        top_row.addWidget(name)
        top_row.addStretch(1)
        top_row.addWidget(time_label)

        preview_text = self.summary.last_message or 'No messages yet'
        preview = QtWidgets.QLabel(elide_text(preview_text, 42))
        preview.setStyleSheet('color: #6b7280; font-size: 12px;')

        layout.addLayout(top_row)
        layout.addWidget(preview)


class MessageRowWidget(QtWidgets.QWidget):
    def __init__(self, message: ChatMessage, is_self: bool):
        super().__init__()
        self.message = message
        self.is_self = is_self
        self._build_ui()

    def _build_ui(self) -> None:
        root = QtWidgets.QHBoxLayout(self)
        root.setContentsMargins(8, 6, 8, 6)
        root.setSpacing(8)

        if self.is_self:
            root.addStretch(1)

        bubble = QtWidgets.QFrame()
        bubble.setMaximumWidth(480)
        bubble_bg = '#2563eb' if self.is_self else '#ffffff'
        bubble_fg = '#ffffff' if self.is_self else '#111827'
        bubble_border = '#2563eb' if self.is_self else '#e5e7eb'
        bubble.setStyleSheet(
            f'QFrame {{ background: {bubble_bg}; color: {bubble_fg}; border: 1px solid {bubble_border}; border-radius: 16px; }}'
        )
        bubble_layout = QtWidgets.QVBoxLayout(bubble)
        bubble_layout.setContentsMargins(14, 10, 14, 8)
        bubble_layout.setSpacing(6)

        text = QtWidgets.QLabel(self.message.body)
        text.setWordWrap(True)
        text.setTextInteractionFlags(QtCore.Qt.TextSelectableByMouse)
        text.setStyleSheet(f'color: {bubble_fg}; font-size: 13px;')

        meta_row = QtWidgets.QHBoxLayout()
        meta_row.setSpacing(8)
        if not self.is_self:
            sender = QtWidgets.QLabel(self.message.author_name)
            sender.setStyleSheet(f'color: {bubble_fg}; font-size: 11px; font-weight: 600;')
            meta_row.addWidget(sender)
            meta_row.addStretch(1)
        else:
            meta_row.addStretch(1)

        meta = QtWidgets.QLabel(self._meta_text())
        meta.setStyleSheet(self._meta_style())
        meta_row.addWidget(meta)

        bubble_layout.addWidget(text)
        bubble_layout.addLayout(meta_row)

        root.addWidget(bubble)
        if not self.is_self:
            root.addStretch(1)

    def _meta_text(self) -> str:
        ts = humanize_message_time(self.message.inserted_at or _to_iso(self.message.server_ts))
        if not self.is_self:
            return ts
        return f'{ts}  {delivery_icon(self.message.delivery_status)}'

    def _meta_style(self) -> str:
        if not self.is_self:
            return 'color: #6b7280; font-size: 11px;'
        return f'color: {delivery_color(self.message.delivery_status)}; font-size: 11px; font-weight: 600;'


class MainWindow(QMainWindow):
    def __init__(self, signals: Signals, state: ChatState):
        super().__init__()
        self.signals = signals
        self.state = state
        self.client_thread: Optional[ChatClientThread] = None
        self.selected_peer_id: Optional[str] = None
        self._build_ui()
        self._connect_signals()

    def _build_ui(self) -> None:
        self.setWindowTitle('Real-Time Messaging System')
        self.resize(1180, 760)

        central = QtWidgets.QWidget()
        root = QtWidgets.QHBoxLayout(central)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(16)

        sidebar_card = QtWidgets.QFrame(objectName='SidebarCard')
        sidebar_card.setMinimumWidth(320)
        sidebar_card.setMaximumWidth(360)
        sidebar_layout = QtWidgets.QVBoxLayout(sidebar_card)
        sidebar_layout.setContentsMargins(18, 18, 18, 18)
        sidebar_layout.setSpacing(12)

        app_title = QtWidgets.QLabel('Chats')
        app_title.setStyleSheet('font-size: 22px; font-weight: 700; color: #111827;')
        self.account_label = QtWidgets.QLabel('Not connected')
        self.account_label.setStyleSheet('color: #6b7280; font-size: 12px;')

        search_row = QtWidgets.QHBoxLayout()
        self.new_chat_edit = QtWidgets.QLineEdit()
        self.new_chat_edit.setPlaceholderText('Start chat with user ID')
        self.new_chat_edit.returnPressed.connect(self.start_chat_clicked)
        self.new_chat_button = QtWidgets.QPushButton('New chat')
        self.new_chat_button.setObjectName('SecondaryButton')
        self.new_chat_button.clicked.connect(self.start_chat_clicked)
        search_row.addWidget(self.new_chat_edit)
        search_row.addWidget(self.new_chat_button)

        self.chat_list_widget = QtWidgets.QListWidget()
        self.chat_list_widget.setSpacing(6)
        self.chat_list_widget.itemClicked.connect(self.on_chat_selected)

        refresh_button = QtWidgets.QPushButton('Refresh chats')
        refresh_button.setObjectName('SecondaryButton')
        refresh_button.clicked.connect(self.refresh_chats)

        sidebar_layout.addWidget(app_title)
        sidebar_layout.addWidget(self.account_label)
        sidebar_layout.addLayout(search_row)
        sidebar_layout.addWidget(self.chat_list_widget, 1)
        sidebar_layout.addWidget(refresh_button)

        right_container = QtWidgets.QVBoxLayout()
        right_container.setSpacing(12)

        header_card = QtWidgets.QFrame(objectName='ChatCard')
        header_layout = QtWidgets.QHBoxLayout(header_card)
        header_layout.setContentsMargins(18, 14, 18, 14)
        self.active_peer_title = QtWidgets.QLabel('Select a chat')
        self.active_peer_title.setStyleSheet('font-size: 18px; font-weight: 700; color: #111827;')
        self.active_peer_subtitle = QtWidgets.QLabel('Choose an existing conversation or start a new one.')
        self.active_peer_subtitle.setStyleSheet('color: #6b7280; font-size: 12px;')
        left_header = QtWidgets.QVBoxLayout()
        left_header.setSpacing(2)
        left_header.addWidget(self.active_peer_title)
        left_header.addWidget(self.active_peer_subtitle)
        header_layout.addLayout(left_header)
        header_layout.addStretch(1)

        self.message_list_widget = QtWidgets.QListWidget()
        self.message_list_widget.setSpacing(2)
        self.message_list_widget.setFrameShape(QtWidgets.QFrame.NoFrame)

        composer_card = QtWidgets.QFrame(objectName='ComposerCard')
        composer_layout = QtWidgets.QVBoxLayout(composer_card)
        composer_layout.setContentsMargins(14, 14, 14, 14)
        composer_layout.setSpacing(10)
        self.composer = QtWidgets.QPlainTextEdit()
        self.composer.setPlaceholderText('Type a message...')
        self.composer.setFixedHeight(96)
        self.send_button = QtWidgets.QPushButton('Send')
        self.send_button.clicked.connect(self.send_current_message)
        send_row = QtWidgets.QHBoxLayout()
        self.helper_label = QtWidgets.QLabel('Enter to send is disabled. Press Send for now.')
        self.helper_label.setStyleSheet('color: #6b7280; font-size: 12px;')
        send_row.addWidget(self.helper_label)
        send_row.addStretch(1)
        send_row.addWidget(self.send_button)
        composer_layout.addWidget(self.composer)
        composer_layout.addLayout(send_row)

        right_container.addWidget(header_card)
        right_container.addWidget(self.message_list_widget, 1)
        right_container.addWidget(composer_card)

        root.addWidget(sidebar_card)
        root.addLayout(right_container, 1)

        self.setCentralWidget(central)
        self.statusBar().showMessage('Ready')
        self._set_chat_enabled(False)

    def _connect_signals(self) -> None:
        self.signals.connected.connect(self.on_connected)
        self.signals.login_failed.connect(self.on_login_failed)
        self.signals.connection_failed.connect(self.on_connection_failed)
        self.signals.chat_started.connect(self.on_chat_started)
        self.signals.chat_start_failed.connect(self.on_chat_start_failed)
        self.signals.chat_list.connect(self.on_chat_list_loaded)
        self.signals.history_loaded.connect(self.on_history_loaded)
        self.signals.message_received.connect(self.on_message_received)
        self.signals.delivery_updated.connect(self.on_delivery_updated)
        self.signals.server_disconnected.connect(self.on_server_disconnected)

    def connect_to_server(self, host: str, port: int, username: str, password: str) -> None:
        if self.client_thread:
            self.client_thread.close()
        self.statusBar().showMessage('Connecting...')
        self.client_thread = ChatClientThread(host=host, port=port, username=username, password=password, signals=self.signals)
        self.client_thread.start()

    def on_connected(self, payload: dict) -> None:
        self.state.set_self(payload['self']['user_id'], payload['self']['username'])
        self.account_label.setText(f"Signed in as {payload['self']['username']}  ·  user ID {payload['self']['user_id']}")
        chats = payload.get('chats') or []
        self.state.load_chat_list(chats)
        self.refresh_chat_list()
        self.statusBar().showMessage('Connected successfully')
        self._set_chat_enabled(False)

    def on_login_failed(self, error: str) -> None:
        self.statusBar().showMessage('Login failed')
        QtWidgets.QMessageBox.warning(self, 'Login failed', error)

    def on_connection_failed(self, error: str) -> None:
        self.statusBar().showMessage(f'Connection problem: {error}')
        QtWidgets.QMessageBox.warning(self, 'Connection problem', error)

    def start_chat_clicked(self) -> None:
        peer_id = self.new_chat_edit.text().strip()
        if not peer_id:
            self.statusBar().showMessage('Enter a user ID to start a chat')
            return
        if not self.client_thread:
            self.statusBar().showMessage('Not connected')
            return
        self.statusBar().showMessage(f'Opening chat with {peer_id}...')
        self.client_thread.start_chat(peer_id)

    def refresh_chats(self) -> None:
        if self.client_thread:
            self.client_thread.fetch_chats()
            self.statusBar().showMessage('Refreshing chat list...')

    def on_chat_started(self, payload: dict) -> None:
        summary = self.state.upsert_chat_summary(payload)
        if summary is None:
            return
        if payload.get('history') is not None:
            self.state.replace_history(summary.chat_id, payload.get('history') or [])
        self.selected_peer_id = summary.peer_id
        self.refresh_chat_list()
        self.render_selected_chat()
        self.statusBar().showMessage(f'Chat ready with {summary.peer_username}')

    def on_chat_start_failed(self, payload: dict) -> None:
        QtWidgets.QMessageBox.information(self, 'Unable to start chat', payload.get('message', 'Unable to start chat'))
        self.statusBar().showMessage(payload.get('message', 'Unable to start chat'))

    def on_chat_list_loaded(self, chats: List[dict]) -> None:
        self.state.load_chat_list(chats)
        self.refresh_chat_list()
        self.statusBar().showMessage('Chat list updated')

    def on_history_loaded(self, payload: dict) -> None:
        try:
            chat_id = int(payload.get('chat_id') or 0)
        except (TypeError, ValueError):
            return
        peer_id = self.state.replace_history(chat_id, payload.get('messages') or [])
        if peer_id and self.selected_peer_id == peer_id:
            self.render_selected_chat()
        self.refresh_chat_list()

    def on_message_received(self, payload: dict) -> None:
        peer_id = self.state.add_inbound_message(payload)
        self.refresh_chat_list()
        if self.selected_peer_id is None:
            self.selected_peer_id = peer_id
        if peer_id == self.selected_peer_id:
            self.render_selected_chat(scroll_to_bottom=True)
        else:
            peer = self.state.peers.get(peer_id)
            self.statusBar().showMessage(f"New message from {peer.username if peer else peer_id}")

    def on_delivery_updated(self, payload: dict) -> None:
        updated = self.state.update_delivery(payload)
        if updated is None:
            return
        message, status = updated
        if self.selected_peer_id == message.recipient_id:
            self.render_selected_chat(scroll_to_bottom=True)
        self.refresh_chat_list()
        if status == 'failed':
            self.statusBar().showMessage('Message failed to send')
        elif status == 'delivered_to_client':
            self.statusBar().showMessage('Message delivered')
        else:
            self.statusBar().showMessage('Message sent')

    def on_server_disconnected(self, notice: str) -> None:
        self.statusBar().showMessage(notice)
        self._set_chat_enabled(False)

    def on_chat_selected(self, item: QtWidgets.QListWidgetItem) -> None:
        peer_id = item.data(QtCore.Qt.UserRole)
        if not peer_id:
            return
        self.selected_peer_id = str(peer_id)
        self.render_selected_chat()
        summary = self.state.get_chat_summary(self.selected_peer_id)
        if self.client_thread and summary:
            self.client_thread.fetch_history(chat_id=summary.chat_id or None, peer_id=self.selected_peer_id, limit=50)

    def send_current_message(self) -> None:
        if not self.selected_peer_id:
            self.statusBar().showMessage('Select or start a chat first')
            return
        body = self.composer.toPlainText().strip()
        if not body:
            return
        self.composer.clear()
        message = self.state.add_outbound_message(self.selected_peer_id, body)
        self.refresh_chat_list()
        self.render_selected_chat(scroll_to_bottom=True)
        if not self.client_thread:
            self.statusBar().showMessage('Not connected')
            return
        self.client_thread.send_chat(self.selected_peer_id, body, message.client_message_id, message.chat_id)
        self.statusBar().showMessage('Sending message...')

    def refresh_chat_list(self) -> None:
        self.chat_list_widget.clear()
        for summary in self.state.sorted_chat_summaries():
            item = QtWidgets.QListWidgetItem()
            item.setData(QtCore.Qt.UserRole, summary.peer_id)
            selected = summary.peer_id == self.selected_peer_id
            widget = ChatPreviewWidget(summary, selected=selected)
            item.setSizeHint(widget.sizeHint())
            self.chat_list_widget.addItem(item)
            self.chat_list_widget.setItemWidget(item, widget)

    def render_selected_chat(self, scroll_to_bottom: bool = False) -> None:
        peer_id = self.selected_peer_id
        if not peer_id:
            self._set_chat_enabled(False)
            self.active_peer_title.setText('Select a chat')
            self.active_peer_subtitle.setText('Choose an existing conversation or start a new one.')
            self.message_list_widget.clear()
            return

        self._set_chat_enabled(True)
        peer = self.state.peers.get(peer_id) or User(peer_id, peer_id)
        self.active_peer_title.setText(peer.username)
        self.active_peer_subtitle.setText(f'user ID {peer.user_id}')
        self.new_chat_edit.setText(peer_id)

        self.message_list_widget.clear()
        messages = self.state.messages_by_peer.get(peer_id, [])
        for message in messages:
            is_self = self.state.self_user is not None and message.author_id == self.state.self_user.user_id
            item = QtWidgets.QListWidgetItem()
            widget = MessageRowWidget(message, is_self=is_self)
            item.setSizeHint(widget.sizeHint())
            self.message_list_widget.addItem(item)
            self.message_list_widget.setItemWidget(item, widget)
        if scroll_to_bottom or messages:
            self.message_list_widget.scrollToBottom()

    def _set_chat_enabled(self, enabled: bool) -> None:
        self.composer.setEnabled(enabled)
        self.send_button.setEnabled(enabled)

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        if self.client_thread:
            self.client_thread.close()
        super().closeEvent(event)


class AppController(QtWidgets.QStackedWidget):
    def __init__(self) -> None:
        super().__init__()
        self.signals = Signals()
        self.state = ChatState()
        self.login_page = LoginPage()
        self.main_window = MainWindow(self.signals, self.state)
        self.addWidget(self.login_page)
        self.addWidget(self.main_window)
        self.setWindowTitle('Real-Time Messaging System')
        self.resize(1180, 760)
        self.login_page.login_requested.connect(self._handle_login_requested)
        self.signals.connected.connect(self._show_main)
        self.signals.connection_failed.connect(self._show_login_feedback)
        self.signals.login_failed.connect(self._show_login_feedback)

    def _handle_login_requested(self, host: str, port: int, username: str, password: str) -> None:
        self.login_page.login_button.setEnabled(False)
        self.login_page.login_button.setText('Connecting...')
        self.login_page.set_feedback('')
        self.main_window.connect_to_server(host, port, username, password)

    def _show_main(self, payload: dict) -> None:
        self.login_page.login_button.setEnabled(True)
        self.login_page.login_button.setText('Connect')
        self.setCurrentWidget(self.main_window)

    def _show_login_feedback(self, error: str) -> None:
        if self.currentWidget() is not self.login_page:
            return
        self.login_page.login_button.setEnabled(True)
        self.login_page.login_button.setText('Connect')
        self.login_page.set_feedback(error)


def delivery_icon(status: str) -> str:
    status = (status or '').lower()
    if status == 'delivered_to_client':
        return '✓✓'
    if status in {'stored', 'queued_to_socket', 'published_to_process'}:
        return '✓'
    if status == 'failed':
        return '!'
    return '○'


def delivery_color(status: str) -> str:
    status = (status or '').lower()
    if status == 'delivered_to_client':
        return '#bfdbfe'
    if status == 'failed':
        return '#fecaca'
    return '#dbeafe'


def elide_text(text: str, max_len: int) -> str:
    text = (text or '').replace('\n', ' ').strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + '…'


def _to_epoch(value) -> float:
    if value is None:
        return time.time()
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()
        except Exception:
            return time.time()
    return time.time()


def _to_iso(epoch_value: float) -> str:
    try:
        return datetime.fromtimestamp(float(epoch_value), tz=timezone.utc).isoformat()
    except Exception:
        return datetime.now(tz=timezone.utc).isoformat()


def _sortable_timestamp(value: Optional[str]) -> float:
    if not value:
        return 0.0
    return _to_epoch(value)


def humanize_timestamp_short(value: Optional[str]) -> str:
    if not value:
        return ''
    dt = datetime.fromtimestamp(_to_epoch(value))
    now = datetime.now()
    if dt.date() == now.date():
        return dt.strftime('%I:%M %p').lstrip('0')
    if dt.date() == now.date().replace(day=now.day):
        return dt.strftime('%I:%M %p').lstrip('0')
    if (now.date() - dt.date()).days < 7:
        return dt.strftime('%a')
    return dt.strftime('%d %b')


def humanize_message_time(value: Optional[str]) -> str:
    if not value:
        return ''
    dt = datetime.fromtimestamp(_to_epoch(value))
    return dt.strftime('%I:%M %p').lstrip('0')


def main() -> int:
    app = QApplication(sys.argv)
    app.setStyleSheet(APP_STYLESHEET)
    controller = AppController()
    controller.show()
    return app.exec_()


if __name__ == '__main__':
    sys.exit(main())
