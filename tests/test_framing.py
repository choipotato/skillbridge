from socket import SHUT_WR, socketpair
from subprocess import check_output
from sys import executable
from threading import Thread
from time import sleep

from pytest import mark, raises

from skillbridge.client.channel import TcpChannel
from skillbridge.server import python_server
from skillbridge.server.protocol import MAX_FRAME_LENGTH, parse_frame_length


class PairChannel(TcpChannel):
    def __init__(self, sock) -> None:  # skip connecting to a real server
        self._max_transmission_length = MAX_FRAME_LENGTH
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
    big = 'y' * (MAX_FRAME_LENGTH - len(b'success '))
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


@mark.parametrize('header', [b'        -1', b'   1000001', b'9999999999', b'not-a-size'])
def test_client_rejects_invalid_length_before_reading_payload(header):
    client, server = socketpair()
    with client, server:
        client.settimeout(0.5)
        channel = PairChannel(client)
        # Changing the outgoing limit must not relax the receive limit.
        channel.max_transmission_length = MAX_FRAME_LENGTH * 2
        server.sendall(header)
        # Leave the peer open without a payload: an attempted read times out.
        with raises(ValueError):
            channel._receive_message()
        assert client.fileno() == -1
        assert not channel.connected


@mark.parametrize('header', [b'        -1', b'   1000001', b'9999999999', b'not-a-size'])
def test_server_rejects_invalid_length_before_reading_payload(header, monkeypatch):
    def unexpected_skill_call(*args):
        raise AssertionError('Invalid frames must not reach SKILL')

    monkeypatch.setattr(python_server, 'send_to_skill', unexpected_skill_call)
    monkeypatch.setattr(python_server, 'read_from_skill', unexpected_skill_call)
    client, server = socketpair()
    with client, server:
        server.settimeout(0.5)
        client.sendall(header)
        assert _make_handler(server).handle_one_request() is False


@mark.parametrize('length', [0, MAX_FRAME_LENGTH])
@mark.parametrize('padding', ['space', 'zero'])
def test_frame_length_accepts_inclusive_boundaries(length, padding):
    header = f'{length:10}' if padding == 'space' else f'{length:010}'
    assert parse_frame_length(header.encode()) == length


def test_client_receives_empty_frame():
    client, server = socketpair()
    with client, server:
        server.sendall(b'         0')
        assert PairChannel(client)._receive_message() == b''


@mark.parametrize('length', [0, MAX_FRAME_LENGTH])
def test_server_accepts_inclusive_payload_boundaries(length, monkeypatch):
    received = []
    monkeypatch.setattr(python_server, 'send_to_skill', received.append)
    monkeypatch.setattr(python_server, 'read_from_skill', lambda _: 'success ok')
    payload = b'x' * length
    client, server = socketpair()
    with client, server:
        server.settimeout(2)
        sender = Thread(target=client.sendall, args=(f'{length:10}'.encode() + payload,))
        sender.start()
        try:
            assert _make_handler(server).handle_one_request() is True
        finally:
            sender.join(2)
        assert not sender.is_alive()
        assert PairChannel(client)._receive_only() == 'ok'
        assert received == [payload.decode()]


def test_server_stops_when_client_dies_mid_length():
    client, server = socketpair()
    with client, server:
        client.sendall(b'    ')
        client.shutdown(SHUT_WR)
        assert _make_handler(server).handle_one_request() is False


def test_server_script_runs_without_installed_package(tmp_path):
    # Isolate sys.path and site-packages to match the documented standalone use.
    output = check_output(
        [executable, '-E', '-S', python_server.__file__, '--help'], cwd=tmp_path, timeout=5
    )
    assert b'usage:' in output
