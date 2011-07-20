#!/usr/bin/env python
from dingus import Dingus, DingusTestCase, DontCare
import nose.tools as nose_tools
import sys
import socket
from stompy import simple
from stompy.simple import Client, TransactionError


class WhenUsingSimpleClient(DingusTestCase(Client,
    exclude=['TransactionError', 'Empty'])):

    def setup(self):
        super(WhenUsingSimpleClient, self).setup()
        self.client = Client()

    def should_connect(self):
        self.client.connect()
        assert self.client.stomp.calls('connect')

    def should_disconnect(self):
        self.client.disconnect()
        assert self.client.stomp.calls('disconnect')

    def should_subscribe(self):
        self.client.subscribe('/queue/nose_test')
        print self.client.stomp.calls
        assert self.client.stomp.calls('subscribe',
                {'ack': 'auto', 'destination': '/queue/nose_test'})

    def should_unsubscribe(self):
        self.client.unsubscribe('/queue/nose_test')
        assert self.client.stomp.calls('unsubscribe',
                {'destination': '/queue/nose_test'})

    def should_begin_transaction(self):
        self.client.begin('bah')
        assert self.client.stomp.calls('begin',
            {"transaction": self.client._current_transaction})

    def should_fail_to_begin_already_in_transaction(self):
        self.client._current_transaction = "meh"
        nose_tools.assert_raises(TransactionError, self.client.begin, 'bah')

    def should_commit_transaction(self):
        self.client._current_transaction = 'meh'
        self.client.commit('bah')
        assert self.client.stomp.calls('commit', {'transaction': 'meh'})

    def should_fail_to_commit_transaction(self):
        nose_tools.assert_raises(TransactionError, self.client.commit, 'bah')

    def should_abort_transaction(self):
        self.client._current_transaction = 'meh'
        self.client.abort()
        assert self.client.stomp.calls('abort', {'transaction': 'meh'})

    def should_fail_to_abort_transaction(self):
        nose_tools.assert_raises(TransactionError, self.client.abort)

    def should_ack_message(self):
        self.client.ack("fake_frame")
        assert self.client.stomp.calls('ack', "fake_frame")

    def should_make_conf(self):
        conf = self.client._make_conf(None,
            destination='/queue/nose_test', ack='auto')
        assert isinstance(conf, type({}))

    def should_make_conf_with_transaction(self):
        self.client._current_transaction = 'meh'
        conf = self.client._make_conf({},
            destination='/queue/nose_test', ack='auto')
        assert isinstance(conf, type({}))

    def should_put_item_into_queue(self):
        self.client.put('bah', '/queue/nose_test')
        conf = self.client._make_conf(None, body='bah',
            destination='/queue/nose_test',
            persistent='true')

        assert self.client.stomp.calls('send', conf)

    def should_get_message(self):
        self.client.get()
        assert self.client.stomp.calls('receive_frame', nonblocking=False, callback=None)

    def should_get_message_without_blocking(self):
        self.client.get_nowait()
        assert self.client.stomp.calls('receive_frame', nonblocking=True, callback=None)

    def should_not_get_message(self):
        self.client.stomp.receive_frame.return_value = None
        nose_tools.assert_raises(self.client.Empty,
            self.client.get, block=False)
