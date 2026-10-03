from socket import socketpair
from threading import Thread
from time import sleep

from pytest import raises

from skillbridge.client.channel import TcpChannel
from skillbridge.server import python_server


class PairChannel(TcpChannel):
    def __init__(self, sock) -> None:  # skip connecting to a real server
        self._max_transmission_length = 1_000_000
        self.connected = False
        self.socket = sock


def _send_in_chunks(sock, data: bytes, size: int) -> None:
    for i in range(0, len(data), size):
        sock.sendall(data[i : i + size])
        if i < 20:  # make sure the length header arrives in separate pieces
            sleep(0.01)


def test_client_receives_message_split_into_tiny_chunks():
    client, server = socketpair()
    channel = PairChannel(client)
    payload = b'success ' + b'x' * 5000
    message = f'{len(payload):10}'.encode() + payload

    t = Thread(target=_send_in_chunks, args=(server, message, 3))
    t.start()
    assert channel._receive_only() == 'x' * 5000
    t.join()
    server.close()


def test_client_raises_when_server_dies_mid_message():
    client, server = socketpair()
    channel = PairChannel(client)
    server.sendall(f'{100:10}'.encode() + b'success ab')
    server.close()

    with raises(RuntimeError, match="unexpectedly died"):
        channel._receive_only()


def test_client_raises_when_server_dies_mid_length():
    client, server = socketpair()
    channel = PairChannel(client)
    server.sendall(b'    ')
    server.close()

    with raises(RuntimeError, match="unexpectedly died"):
        channel._receive_only()


class _FakeServer:
    skill_timeout = None


def _make_handler(sock):
    handler = python_server.Handler.__new__(python_server.Handler)
    handler.request = sock
    handler.client_address = 'test'
    handler.server = _FakeServer()
    return handler


def test_server_handles_split_request_and_sends_whole_response(monkeypatch):
    client, server = socketpair()
    received = []
    big = 'y' * 3_000_000
    monkeypatch.setattr(python_server, 'send_to_skill', received.append)
    monkeypatch.setattr(python_server, 'read_from_skill', lambda _: f'success {big}')

    command = b'some command'
    t = Thread(target=_send_in_chunks, args=(client, f'{len(command):10}'.encode() + command, 1))
    t.start()

    result = []
    s = Thread(target=lambda: result.append(_make_handler(server).handle_one_request()))
    s.start()

    channel = PairChannel(client)
    assert channel._receive_only() == big
    s.join()
    t.join()

    assert result == [True]
    assert received == ['some command']


def test_server_stops_when_client_dies_mid_request():
    client, server = socketpair()
    client.sendall(f'{100:10}'.encode() + b'abc')
    client.close()

    assert _make_handler(server).handle_one_request() is False
