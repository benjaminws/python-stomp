#!/usr/bin/env python
from dingus import Dingus, DingusTestCase, DontCare
from unittest import TestCase
import sys
import stompy
from stompy import Stomp


class WhenConnecting(DingusTestCase(Stomp)):

    def setup(self):
        super(WhenConnecting, self).setup()
        self.host = 'localhost'
        self.port = 61613
        self.stomp = Stomp(self.host, self.port)
        self.sock = self.stomp.sock
        self.frame = self.stomp.frame
        self.stomp.connected = True

    def should_set_socket_opts(self):
        assert stompy.stomp.socket.calls('socket', DontCare, DontCare)

    def should_connect(self):
        self.stomp.connect()
        assert self.frame.calls('connect', self.sock)
        assert self.sock.calls('connect', (self.host, self.port))

    def should_disconnect(self):
        self.stomp.disconnect()
        built_frame = self.frame.calls('build_frame', DontCare).one()
        built_frame_args = built_frame.args[0]
        send_args = built_frame[3]

        assert built_frame_args['command'] == 'DISCONNECT'
        assert self.frame.calls('send_frame', send_args.as_string())
        assert self.stomp.sock.calls('shutdown', 0)

    def should_raise_not_connected_error(self):
        self.stomp.disconnect()


class WhenProducingMessages(DingusTestCase(Stomp)):

    def setup(self):
        super(WhenProducingMessages, self).setup()
        self.host = 'localhost'
        self.port = 61613
        self.stomp = Stomp(self.host, self.port)
        self.frame = self.stomp.frame
        self.sock = self.stomp.sock
        self.stomp.connected = True
        self.headers = {'destination': '/queue/nose_test',
                        'body': 'test'}

    def should_build_frame_and_send(self):
        self.stomp.send(self.headers)
        built_frame = self.frame.calls('build_frame', DontCare).one()
        built_frame_args = built_frame.args[0]
        send_args = built_frame[3]

        assert built_frame_args['command'] == 'SEND'
        assert self.frame.calls('send_frame', send_args.as_string())


class WhenUsingTransactions(DingusTestCase(Stomp)):

    def setup(self):
        super(WhenUsingTransactions, self).setup()
        self.host = 'localhost'
        self.port = 61613
        self.stomp = Stomp(self.host, self.port)
        self.frame = self.stomp.frame
        self.sock = self.stomp.sock
        self.stomp.connected = True
        self.headers = {'transaction': 'nose_123'}

    def should_begin(self):
        self.stomp.begin(self.headers)
        built_frame = self.frame.calls('build_frame', DontCare).one()
        built_frame_args = built_frame.args[0]
        send_args = built_frame[3]

        assert built_frame_args['command'] == 'BEGIN'
        assert self.frame.calls('send_frame', send_args.as_string())

    def should_commit(self):
        self.stomp.commit(self.headers)
        built_frame = self.frame.calls('build_frame', DontCare).one()
        built_frame_args = built_frame.args[0]
        send_args = built_frame[3]

        assert built_frame_args['command'] == 'COMMIT'
        assert self.frame.calls('send_frame', send_args.as_string())

    def should_abort(self):
        self.stomp.abort(self.headers)
        built_frame = self.frame.calls('build_frame', DontCare).one()
        built_frame_args = built_frame.args[0]
        send_args = built_frame[3]

        assert built_frame_args['command'] == 'ABORT'
        assert self.frame.calls('send_frame', send_args.as_string())


class WhenConsumingMessages(DingusTestCase(Stomp)):

    def setup(self):
        super(WhenConsumingMessages, self).setup()
        self.host = 'localhost'
        self.port = 61613
        self.stomp = Stomp(self.host, self.port)
        self.frame = self.stomp.frame
        self.sock = self.stomp.sock
        self.stomp.connected = True
        self.headers = {'destination': '/queue/nose_test',
                        'ack': 'client'}

    def should_subscribe(self):
        self.stomp.subscribe(self.headers)
        built_frame = self.frame.calls('build_frame', DontCare).one()
        built_frame_args = built_frame.args[0]
        send_args = built_frame[3]

        assert self.stomp.subscribed
        assert built_frame_args['command'] == 'SUBSCRIBE'
        assert self.frame.calls('send_frame', send_args.as_string())

    def should_poll(self):
        this_frame = self.stomp.poll()
        assert self.stomp.frame.calls('get_message', nb=True)

    def should_receive_and_ack(self):
        this_frame = self.stomp.receive_frame()
        assert self.stomp.frame.calls('get_message')

        self.stomp.ack(this_frame)
        built_frame = self.frame.calls('build_frame', DontCare).one()
        built_frame_args = built_frame.args[0]
        send_args = built_frame[3]

        assert built_frame_args['command'] == 'ACK'
        assert self.frame.calls('send_frame', send_args.as_string())

    def should_register_callback(self):
        callback_func = lambda message: 'pass'
        this_frame = self.stomp.receive_frame(callback=callback_func)
        assert self.stomp.frame.calls('get_message')
        assert isinstance(self.stomp._callback, type(callback_func))

    def should_unsubscribe(self):
        self.stomp.unsubscribe({'destination': '/queue/nose-test'})
        built_frame = self.frame.calls('build_frame', DontCare).one()
        built_frame_args = built_frame.args[0]
        send_args = built_frame[3]

        assert built_frame_args['command'] == 'UNSUBSCRIBE'
        assert self.frame.calls('send_frame', send_args.as_string())
        assert not self.stomp.subscribed

    def should_unsub_via_disco(self):
        self.stomp._subscribed_to["/queue/nose-test"] = True
        self.stomp.disconnect()
        assert not self.stomp.subscribed


class WhenUsingProperties(TestCase):

    def should_set_sub(self):
        mystomp = Stomp('localhost', 99999)
        mystomp._subscribed_to['/queue/nose_test'] = True
        assert mystomp.subscribed is not None


class WhenNotConnected(TestCase):

    def should_fail_to_send(self):
        mystomp = Stomp('localhost', 99999)
        self.failUnlessRaises(stompy.NotConnectedError, mystomp.send,
                              {"body": "f"})

    def should_raise_nc_when_sending(self):
        mystomp = Stomp('localhost', 99999)
        try:
            mystomp.send({"body": "Vandelay Industries"})
        except stompy.NotConnectedError, err:
            assert True # Should raise not connected
            return
        assert False # Should raise not connected

    def should_set_disconnected_even_when_nc(self):
        mystomp = Stomp('localhost', 99999)
        mystomp.disconnect()
        assert not mystomp.connected


class WhenSocketCantConnect(TestCase):

    def should_fail_connect(self):
        self.stomp = Stomp('localhost', 65535)
        self.failUnlessRaises(self.stomp.ConnectionError, self.stomp.connect)

    def should_fail_connect_with_timeout(self):
        import socket
        socket.setdefaulttimeout(.5)
        self.stomp = Stomp('10.10.0.0', 65535)
        self.failUnlessRaises(self.stomp.ConnectionTimeoutError,
            self.stomp.connect)
