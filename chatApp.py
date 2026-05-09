import socket
import sys
import threading
from PyQt5 import QtCore, QtWidgets

from chatapp_core.protocol import recv_message, send_message


class ClientThread(QtCore.QThread):
    received = QtCore.pyqtSignal(dict)
    disconnected = QtCore.pyqtSignal(str)

    def __init__(self, host, port, user_id):
        super().__init__()
        self.host = host
        self.port = port
        self.user_id = user_id
        self.sock = None

    def run(self):
        try:
            self.sock = socket.create_connection((self.host, self.port), timeout=10)
            send_message(self.sock, {'type': 'login', 'user_id': self.user_id})
            while True:
                self.received.emit(recv_message(self.sock))
        except Exception as exc:
            self.disconnected.emit(str(exc))

    def send_payload(self, payload):
        if self.sock:
            send_message(self.sock, payload)


class ChatWindow(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('Real-Time Messaging System')
        self.host = QtWidgets.QLineEdit('127.0.0.1')
        self.port = QtWidgets.QSpinBox(); self.port.setMaximum(65535); self.port.setValue(12345)
        self.user = QtWidgets.QLineEdit('1')
        self.peer = QtWidgets.QLineEdit('2')
        self.text = QtWidgets.QLineEdit()
        self.log = QtWidgets.QTextEdit(); self.log.setReadOnly(True)
        self.connect_btn = QtWidgets.QPushButton('Connect')
        self.send_btn = QtWidgets.QPushButton('Send')
        form = QtWidgets.QFormLayout()
        form.addRow('Host', self.host); form.addRow('Port', self.port); form.addRow('User', self.user); form.addRow('Peer', self.peer)
        layout = QtWidgets.QVBoxLayout(self)
        layout.addLayout(form); layout.addWidget(self.log); layout.addWidget(self.text)
        row = QtWidgets.QHBoxLayout(); row.addWidget(self.connect_btn); row.addWidget(self.send_btn); layout.addLayout(row)
        self.client = None
        self.connect_btn.clicked.connect(self.connect_to_server)
        self.send_btn.clicked.connect(self.send_chat)

    def connect_to_server(self):
        self.client = ClientThread(self.host.text(), self.port.value(), self.user.text())
        self.client.received.connect(self.on_message)
        self.client.disconnected.connect(lambda error: self.log.append(f'Disconnected: {error}'))
        self.client.start()
        self.log.append('Connecting...')

    def send_chat(self):
        if not self.client:
            return
        text = self.text.text().strip()
        if not text:
            return
        self.client.send_payload({'type': 'send_message', 'receiver_id': self.peer.text(), 'text': text})
        self.log.append(f'Me -> {self.peer.text()}: {text}')
        self.text.clear()

    def on_message(self, payload):
        if payload.get('type') == 'message':
            self.log.append(f"{payload.get('sender_id')}: {payload.get('text')}")
        else:
            self.log.append(str(payload))


def main():
    app = QtWidgets.QApplication(sys.argv)
    win = ChatWindow(); win.resize(640, 480); win.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
