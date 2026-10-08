from io import StringIO
from pathlib import Path
from socket import AF_INET, SHUT_WR, SOCK_STREAM, socket, socketpair
from subprocess import check_output
from sys import executable
from threading import Thread
from time import sleep

from pytest import fixture, mark, raises

from skillbridge.client.channel import TcpChannel, create_channel_class
from skillbridge.server import python_server
from skillbridge.server.protocol import MAX_FRAME_LENGTH, parse_frame_length


@fixture(params=['unix', 'tcp'])
def frame_sockets(request):
    if request.param == 'unix':
        client, server = socketpair()
        with client, server:
            yield client, server
        return

    with socket(AF_INET, SOCK_STREAM) as listener, socket(AF_INET, SOCK_STREAM) as client:
        listener.bind(('127.0.0.1', 0))
        listener.listen(1)
        client.connect(listener.getsockname())
        server, _ = listener.accept()
        with server:
            yield client, server


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


def test_client_receives_message_split_into_tiny_chunks(frame_sockets):
    client, server = frame_sockets
    channel = PairChannel(client)
    payload = b'success ' + b'x' * 5000
    message = f'{len(payload):10}'.encode() + payload

    t = Thread(target=_send_in_chunks, args=(server, message, 3))
    t.start()
    assert channel._receive_only() == 'x' * 5000
    t.join()
    server.close()


def test_client_raises_when_server_dies_mid_message(frame_sockets):
    client, server = frame_sockets
    channel = PairChannel(client)
    server.sendall(f'{100:10}'.encode() + b'success ab')
    server.close()

    with raises(RuntimeError, match="unexpectedly died"):
        channel._receive_only()


def test_client_raises_when_server_dies_mid_length(frame_sockets):
    client, server = frame_sockets
    channel = PairChannel(client)
    server.sendall(b'    ')
    server.close()

    with raises(RuntimeError, match="unexpectedly died"):
        channel._receive_only()


def _make_handler(sock):
    handler_class = python_server.create_handler(lambda: True)
    handler = handler_class.__new__(handler_class)
    handler.request = sock
    handler.client_address = 'test'
    return handler


def test_server_handles_split_request_and_sends_whole_response(monkeypatch, frame_sockets):
    client, server = frame_sockets
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


def test_server_stops_when_client_dies_mid_request(frame_sockets):
    client, server = frame_sockets
    client.sendall(f'{100:10}'.encode() + b'abc')
    client.close()

    assert _make_handler(server).handle_one_request() is False


@mark.parametrize('header', [b'        -1', b'   1000001', b'9999999999', b'not-a-size'])
def test_client_rejects_invalid_length_before_reading_payload(header, frame_sockets):
    client, server = frame_sockets
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
def test_server_rejects_invalid_length_before_reading_payload(header, monkeypatch, frame_sockets):
    def unexpected_skill_call(*args):
        raise AssertionError('Invalid frames must not reach SKILL')

    monkeypatch.setattr(python_server, 'send_to_skill', unexpected_skill_call)
    monkeypatch.setattr(python_server, 'read_from_skill', unexpected_skill_call)
    client, server = frame_sockets
    with client, server:
        server.settimeout(0.5)
        client.sendall(header)
        assert _make_handler(server).handle_one_request() is False


@mark.parametrize('length', [0, MAX_FRAME_LENGTH])
@mark.parametrize('padding', ['space', 'zero'])
def test_frame_length_accepts_inclusive_boundaries(length, padding):
    header = f'{length:10}' if padding == 'space' else f'{length:010}'
    assert parse_frame_length(header.encode()) == length


def test_client_receives_empty_frame(frame_sockets):
    client, server = frame_sockets
    with client, server:
        server.sendall(b'         0')
        assert PairChannel(client)._receive_message() == b''


@mark.parametrize('length', [0, MAX_FRAME_LENGTH])
def test_server_accepts_inclusive_payload_boundaries(length, monkeypatch, frame_sockets):
    received = []
    monkeypatch.setattr(python_server, 'send_to_skill', received.append)
    monkeypatch.setattr(python_server, 'read_from_skill', lambda _: 'success ok')
    payload = b'x' * length
    client, server = frame_sockets
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


def test_server_stops_when_client_dies_mid_length(frame_sockets):
    client, server = frame_sockets
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
    assert b'--force-tcp' in output


def test_server_import_does_not_require_unix_socketserver(tmp_path):
    script = (
        'import runpy, socketserver, sys; '
        'del socketserver.UnixStreamServer; '
        'sys.path.insert(0, sys.argv[1]); '
        'runpy.run_path(sys.argv[2])'
    )
    check_output(
        [
            executable,
            '-E',
            '-S',
            '-c',
            script,
            str(Path(python_server.__file__).parent),
            python_server.__file__,
        ],
        cwd=tmp_path,
        timeout=5,
    )


@mark.parametrize('platform', ['linux', 'win32'])
@mark.parametrize('force_tcp', [False, True])
@mark.parametrize('single', [False, True])
def test_server_factory_binds_readiness_and_closes_server(monkeypatch, platform, force_tcp, single):
    calls = []

    class FakeServer:
        def __init__(self, id_, handler):
            calls.append(('id', id_))
            self.handler = handler

        def __enter__(self):
            return self

        def __exit__(self, *args):
            calls.append('closed')

    def tcp_factory(single_):
        calls.append(('tcp', single_))
        return FakeServer

    def unix_factory(single_):
        calls.append(('unix', single_))
        return FakeServer

    def ready(timeout):
        calls.append(('timeout', timeout))
        return False

    monkeypatch.setattr(python_server, 'platform', platform)
    monkeypatch.setattr(python_server, 'create_tcp_server_class', tcp_factory)
    monkeypatch.setattr(python_server, 'create_unix_server_class', unix_factory)
    monkeypatch.setattr(python_server, 'unix_data_ready', ready)
    monkeypatch.setattr(python_server, 'stdin', StringIO('success ok\n'))
    monkeypatch.setattr(python_server, 'send_to_skill', lambda _: None)
    with python_server.create_server('12345', 'WARNING', single, 0.25, force_tcp) as server:
        client, sock = socketpair()
        with client, sock:
            handler = server.handler.__new__(server.handler)
            handler.request = sock
            handler.client_address = 'test'
            client.sendall(b'         4ping')
            assert handler.handle_one_request() is True
            if platform == 'win32':
                assert PairChannel(client)._receive_only() == 'ok\n'
            else:
                with raises(RuntimeError, match='Timeout'):
                    PairChannel(client)._receive_only()
                assert ('timeout', 0.25) in calls
    transport = 'tcp' if platform == 'win32' or force_tcp else 'unix'
    assert calls[0] == (transport, single)
    assert calls[-1] == 'closed'


@mark.parametrize('id_', [None, '7777', 7777, '0', '65535'])
def test_force_tcp_channel_accepts_port_ids(id_):
    channel_class = create_channel_class(force_tcp=True)
    assert channel_class.address_family == AF_INET
    assert channel_class.create_address(id_) == ('localhost', 7777 if id_ is None else int(id_))


@mark.parametrize('id_', ['default', '-1', '65536', '1.5', '\uff11\uff12\uff13'])
def test_force_tcp_channel_rejects_invalid_port_ids(id_):
    with raises(ValueError, match='numeric id'):
        create_channel_class(force_tcp=True).create_address(id_)
