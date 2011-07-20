#!/usr/bin/env python
from dingus import Dingus, DingusTestCase, DontCare
import nose.tools as nose_tools
import sys
import socket
from stompy import frame
from stompy.frame import Frame, IntermediateMessageQueue, \
        UnknownBrokerResponseError, BrokerErrorResponse
from Queue import Empty as QueueEmpty


class WhenSettingUp(DingusTestCase(Frame)):

    def setup(self):
        super(WhenSettingUp, self).setup()
        self.frame = Frame()
        self.sockobj = frame.socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def should_set_name(self):
        assert frame.socket.calls('gethostbyname', frame.socket.gethostname())

    def should_connect(self):
        self.frame._getline = Dingus()
        self.frame._getline.return_value = \
            'CONNECTED\nsession:ID:nose-session123\n\n\x00\n'
        self.frame.connect(self.sockobj.connect('localhost', 99999))
        sendall = self.frame.sock.calls('sendall', DontCare).one().args[0]

        assert self.frame.session is not None
        assert 'CONNECT' in sendall

    def should_connect_with_authentication(self):
        self.frame._getline = Dingus()
        self.frame._getline.return_value = \
            'CONNECTED\nsession:ID:nose-session123\n\n\x00\n'
        self.frame.connect(self.sockobj.connect('localhost', 99999),
                                                username="test",
                                                password="test")
        sendall = self.frame.sock.calls('sendall', DontCare).one().args[0]

        assert self.frame.session is not None
        assert 'login:test' in sendall

    def should_set_client_id(self):
        self.frame._getline = Dingus()
        self.frame._getline.return_value = \
            'CONNECTED\nsession:ID:nose-session123\n\n\x00\n'
        self.frame.connect(self.sockobj.connect('localhost', 99999),
                                                clientid="test")
        sendall = self.frame.sock.calls('sendall', DontCare).one().args[0]

        assert self.frame.session is not None
        assert 'client-id:test' in sendall


class WhenSendingFrames(DingusTestCase(Frame)):

    def setup(self):
        super(WhenSendingFrames, self).setup()
        self.frame = Frame()
        self.sockobj = frame.socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def should_send_frame_and_return_none(self):
        self.frame._getline = Dingus()
        self.frame._getline.return_value = \
                'CONNECTED\nsession:ID:nose-session123\n\n\x00\n'
        self.frame.connect(self.sockobj.connect('localhost', 99999))
        this_frame = self.frame.build_frame({'command': 'CONNECT',
                                             'headers': {}})
        send_frame = self.frame.send_frame(this_frame.as_string())

        assert send_frame is None

    def should_send_frame_and_return_frame(self):
        my_frame = Frame()
        my_frame._getline = Dingus()
        headers = {'destination': '/queue/nose_test',
                   'persistent': 'true'}
        body = {'body': 'Testing'}
        my_frame._getline.return_value = \
                'CONNECTED\nsession:ID:nose-session123\n\n\x00\n'
        my_frame.connect(self.sockobj.connect('localhost', 99999))
        this_frame = my_frame.build_frame({'command': 'SEND',
                                           'headers': headers,
                                           'body': body},
                                           want_receipt=True)
        my_frame._getline.return_value = \
                'RECEIPT\nreceipt-id:ID:nose-receipt123\n\n\x00\n'
        send_frame = my_frame.send_frame(this_frame.as_string())

        assert isinstance(my_frame, Frame)


class WhenBuildingFrames(DingusTestCase(Frame)):

    def setup(self):
        super(WhenBuildingFrames, self).setup()
        self.frame = Frame()
        self.sockobj = frame.socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def should_build_frame(self):
        this_frame = self.frame.build_frame({'command': 'CONNECT',
                                             'headers': {}})

        assert self.frame.command is not None
        assert self.frame.headers is not None
        assert '\x00' in this_frame.as_string()

    def should_build_frame_with_body(self):
        headers = {'destination': '/queue/nose_test',
                   'persistent': 'true'}
        body = 'Testing'
        this_frame = self.frame.build_frame({'command': 'SEND',
                                             'headers': headers,
                                             'body': body})

        assert self.frame.body is not None

    def should_build_frame_with_receipt(self):
        headers = {'destination': '/queue/nose_test',
                   'persistent': 'true'}
        body = 'Testing'
        self.frame.session = {'session': 'ID:nose-session123'}
        this_frame = self.frame.build_frame({'command': 'SEND',
                                             'headers': headers,
                                             'body': body},
                                             want_receipt=True)

        assert 'receipt' in self.frame.headers

    def should_build_frame_bytes_message(self):
        headers = {'destination': '/queue/nose_test',
                   'persistent': 'true',
                   'bytes_message': 'true'}
        body = 'Testing'
        this_frame = self.frame.build_frame({'command': 'SEND',
                                             'headers': headers,
                                             'body': body})

        assert 'content-length:%i' % len(body) in this_frame.as_string()


class WhenUsingIntermediateMQueue(DingusTestCase(IntermediateMessageQueue,
    exclude=['Queue', 'QueueEmpty'])):

    def setup(self):
        super(WhenUsingIntermediateMQueue, self).setup()
        self.queue = IntermediateMessageQueue()
        self.frame = Frame()
        self.frame.sock = Dingus()

    def should_put_into_queue_and_get(self):
        headers = {'destination': '/queue/nose_test',
                   'persistent': 'true',
                   'bytes_message': 'true'}
        body = 'Testing'

        this_frame = self.frame.build_frame({'command': 'SEND',
                                             'headers': headers,
                                             'body': body})

        self.queue.put(this_frame)
        extracted_frame = self.queue.get(this_frame)

        assert 'destination' in extracted_frame.headers

    def should_not_put_into_queue(self):
        headers = {'persistent': 'true',
                   'bytes_message': 'true'}
        body = 'Testing'
        this_frame = self.frame.build_frame({'command': 'SEND',
                                             'headers': headers,
                                             'body': body})

        this_frame = Dingus()
        self.queue.put(this_frame)
        ret_frame = self.queue.get(this_frame)
        assert this_frame.calls('parse_frame', nb=False)

    def should_not_get_from_queue(self):
        headers = {'destination': '/queue/nose_test',
                   'persistent': 'true',
                   'bytes_message': 'true'}
        body = 'Testing'

        this_frame = self.frame.build_frame({'command': 'SEND',
                                             'headers': headers,
                                             'body': body})

        this_frame = Dingus()
        extracted_frame = self.queue.get(this_frame)
        print this_frame.calls
        assert this_frame.calls('parse_frame', nb=False)

    def should_not_get_message(self):
        my_frame = Frame()
        my_frame._getline = Dingus()
        my_frame._getline.return_value = None
        my_frame.iqueue.get = Dingus()
        my_frame.iqueue.get.return_value = None
        ret_frame = my_frame.get_message(nb=True)
        assert ret_frame is None

    def should_get_message_and_return_frame(self):
        my_frame = Frame()
        my_frame._getline = Dingus()
        command = 'MESSAGE'
        body = 'Test 1'
        headers = {'session': 'ID:nose-session123',
                   'content-length': '%d' %len(body)}
        my_frame.parse_frame = Dingus()
        this_frame = my_frame.build_frame({'command': command,
                                           'headers': headers,
                                           'body': body})
        my_frame.parse_frame.return_value = this_frame
        ret_frame = my_frame.get_message(nb=True)
        assert isinstance(ret_frame, Frame)

    def should_get_reply(self):
        my_frame = Frame()
        command = 'SEND'
        body = 'Test 1'
        headers = {'destination': '/queue/nose_test',
                   'persistent': 'true',
                   'bytes_message': 'true'}
        my_frame.parse_frame = Dingus()
        this_frame = my_frame.build_frame({'command': command,
                                           'headers': headers,
                                           'body': body})
        my_frame.parse_frame.return_value = this_frame
        my_frame.rqueue.put_nowait(my_frame)
        ret_frame = my_frame.get_reply(nb=True)
        assert isinstance(ret_frame, Frame)

    def should_not_get_reply(self):
        my_frame = Frame()
        command = 'SEND'
        body = 'Test 1'
        headers = {'destination': '/queue/nose_test',
                   'persistent': 'true',
                   'bytes_message': 'true'}
        my_frame.parse_frame = Dingus()
        this_frame = my_frame.build_frame({'command': command,
                                           'headers': headers,
                                           'body': body})
        my_frame.parse_frame.return_value = None
        ret_frame = my_frame.get_reply(nb=True)
        assert ret_frame is None


class WhenParsingFrames(DingusTestCase(Frame,
        exclude=["UnknownBrokerResponseError", "BrokerErrorResponse"])):

    def setup(self):
        super(WhenParsingFrames, self).setup()
        self.frame = Frame()
        self.sockobj = frame.socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def should_parse_headers(self):
        header = 'destination:/queue/nose_test'
        parsed = self.frame.parse_headers(header)

        assert isinstance(parsed, type({}))

    def should_parse_command(self):
        command_str = 'CONNECT\nsession:ID:nose-session123'
        command = self.frame.parse_command(command_str)

        assert isinstance(command, type(''))

    def should_set_bytes_message(self):
        my_frame = Frame()
        my_frame._getline = Dingus()
        body = 'Test 1'
        my_frame._getline.return_value = (
                'MESSAGE\nsession:ID:nose-session123'
                '\ncontent-length:%d\n\n%s\x00\n' % (len(body), body))
        this_frame = my_frame.parse_frame()

        assert 'bytes_message' in this_frame.headers

    def should_get_line(self):
        command = 'CONNECTED'
        headers = {'session': 'ID:nose-session123'}
        body = '\x00'
        my_frame = Frame()
        self.frame.parse_frame = Dingus()
        this_frame = my_frame.build_frame({'command': command,
                                           'headers': headers,
                                           'body': body})
        self.frame.parse_frame.return_value = this_frame
        self.frame.connect(self.sockobj.connect(('localhost', 99999)))
        header = "session:%(session)s\n" % headers
        ret = '\n'.join([command, header, body])
        self.frame.sock.recv.return_value = ret
        self.frame._getline()

        assert self.frame.sock.calls('recv', 1)

    def should_not_get_line(self):
        my_frame = Frame()
        my_frame._getline = Dingus()
        my_frame._getline.return_value = None
        ret_value = my_frame.parse_frame()
        assert ret_value is None

    def should_fail_to_get_headers(self):
        my_frame = Frame()
        my_frame._getline = Dingus()
        my_frame._getline.return_value = \
                'RECEIPTreceipt-id:ID:nose-receipt123'

        nose_tools.assert_raises(UnknownBrokerResponseError,
            my_frame.parse_frame)

    def should_get_error_from_broker(self):
        my_frame = Frame()
        my_frame._getline = Dingus()
        command = 'ERROR'
        header = 'message:Illegal command'
        body = 'Error Message'
        my_frame._getline.return_value = \
            '%s\n%s\ncontent-length:%d\n\n%s\n\x00' % (command,
                                                       header,
                                                       len(body),
                                                       body)

        nose_tools.assert_raises(BrokerErrorResponse, my_frame.parse_frame)

    def should_return_frame_repr(self):
        my_frame = Frame()
        assert isinstance(repr(my_frame), type(''))
