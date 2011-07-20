import socket
import random
from pprint import pformat
from errno import EAGAIN, EWOULDBLOCK
from Queue import Queue
from Queue import Empty as QueueEmpty


class UnknownBrokerResponseError(Exception):
    """An unexpected response was received from the broker."""


class BrokerErrorResponse(Exception):
    """Received error from the broker."""


class IntermediateMessageQueue(object):
    """Internal message queue that holds messages received by the server.

    This to make sure a message isn't received instead of a command response
    after issuing a receipt request.

    """

    def __init__(self):
        self._queue = Queue()

    def put(self, frame):
        """Put a new frame onto the message queue.
        :param frame: A :class:`Frame` instance.

        """
        if "destination" not in frame.headers:
            return
        self._queue.put(frame)

    def get(self, frame, nb=False):
        """Get a new frame from the message queue.
        If no frame is available it try to get the next frame
        from the socket.

        :param frame: A :class:`Frame` instance.
        :keyword nb: Non-blocking.

        """
        try:
            return self._queue.get_nowait()
        except QueueEmpty:
            return frame.parse_frame(nb=nb)


class Frame(object):
    """Build and manage a STOMP Frame.

    :keyword sock: An open socket to the STOMP server.

    """

    def __init__(self, sock=None):
        self.command = None
        self.headers = {}
        self.body = None
        self.session = None
        self.my_name = socket.gethostbyname(socket.gethostname())
        self.sock = sock
        self.iqueue = IntermediateMessageQueue()
        self.rqueue = Queue()

    def connect(self, sock, username=None, password=None, clientid=None):
        """Connect to the STOMP server and get the session id.

        :param sock: Socket object from stompy.stomp.Stomp.
        :keyword username: Username for connection.
        :keyword password: Password for connection.
        :keyword clientid: Client identification for persistent connections

        """
        self.sock = sock

        headers = {}

        if username and password:
            headers.update({'login': username,
                           'passcode': password})
	
        if clientid:        
            headers.update({'client-id' : clientid})

                    
        frame = self.build_frame({"command": "CONNECT", "headers": headers})

        self.send_frame(frame.as_string())

        # Get session from the next reply from the server.
        next_frame = self.get_reply()
        self.session = next_frame.headers

    def build_frame(self, args, want_receipt=False):
        """Build a frame based on a :class:`dict` of arguments.

        :param args: A :class:`dict` of arguments for the frame.

        :keyword want_receipt: Optional argument to get a receipt from
            the sever that the frame was received.

        Example

            >>> frame = frameobj.build_frame({"command": 'CONNECT',
                                              "headers": {},
                                              want_receipt=True)
        """
        self.command = args.get('command')
        self.headers = args.get('headers')
        self.body = args.get('body')
        if want_receipt:
            receipt_stamp = str(random.randint(0, 10000000))
            self.headers["receipt"] = "%s-%s" % (
                    self.session.get("session"), receipt_stamp)
        return self

    def as_string(self):
        """Raw string representation of this frame
        Suitable for passing over a socket to the STOMP server.

        Example

            >>> stomp.send(frameobj.as_string())

        """
        command = self.command
        headers = self.headers
        body = self.body

        bytes_message = False
        if 'bytes_message' in headers:
            bytes_message = True
            del headers['bytes_message']
            headers['content-length'] = len(body)
        headers['x-client'] = self.my_name

        # Convert and append any existing headers to a string as the
        # protocol describes.
        headerparts = ("%s:%s\n" % (key, value)
                            for key, value in headers.iteritems())

        # Frame is Command + Header + EOF marker.
        frame = "%s\n%s\n%s\x00" % (command, "".join(headerparts), body)

        return frame

    def get_message(self, nb=False):
        """Get next message frame.

        :keyword nb: Non-blocking: If this is set and there is no
            messages currently waiting, this functions returns ``None``
            instead of waiting for more data.

        """
        while True:
            frame = self.iqueue.get(self, nb=nb)
            if not frame and nb:
                return None
            if frame.command == "MESSAGE":
                return frame
            else:
                self.rqueue.put(frame)

    def get_reply(self, nb=False):
        """Get command reply frame.

        :keyword nb: Non-blocking: If this is set and there is no
            messages currently waiting, this functions returns ``None``
            instead of waiting for more data.

        """
        while True:
            try:
                return self.rqueue.get_nowait()
            except QueueEmpty:
                frame = self.parse_frame(nb=nb)
                if not frame and nb:
                    return None
                if frame.command == "MESSAGE":
                    self.iqueue.put(frame)
                else:
                    self.rqueue.put(frame)

    def parse_frame(self, nb=False):
        """Parse data from socket

        :keyword nb: Non-blocking: If this is set and there is no
            messages currently waiting, this functions returns ``None``
            instead of waiting for more data.

        Example

            >>> frameobj.parse_frame()

        """
        line = self._getline(nb=nb)
        if not line:
            return

        command = self.parse_command(line)
        line = line[len(command)+1:]
        headers_str, _, body = line.partition("\n\n")
        if not headers_str:
            raise UnknownBrokerResponseError(
                    "Received: (%s)" % line)
        headers = self.parse_headers(headers_str)

        if 'content-length' in headers:
            headers['bytes_message'] = True

        if command == 'ERROR':
            raise BrokerErrorResponse(
                    "Broker Returned Error: %s" % body)

        frame = Frame(self.sock)
        return frame.build_frame({'command': command,
                                  'headers': headers,
                                  'body': body})

    def parse_command(self, command_str):
        """Parse command received from the server.

        :param command_str: String to parse command from

        """
        command = command_str.split('\n', 1)[0]
        return command

    def parse_headers(self, headers_str):
        """Parse headers received from the servers and convert
        to a :class:`dict`.i

        :param headers_str: String to parse headers from

        """
        # george:constanza\nelaine:benes
        # -> {"george": "constanza", "elaine": "benes"}
        return dict(line.split(":", 1) for line in headers_str.split("\n"))

    def send_frame(self, frame):
        """Send frame to server, get receipt if needed.

        :param frame: :class:`Frame` instance to pass across the socket

        """
        self.sock.sendall(frame)

        if 'receipt' in self.headers:
            return self.get_reply()

    def _getline(self, nb=False):
        """Get a single line from socket

        :keyword nb: Non-blocking: If this is set, and there are no
            messages to receive, this function returns ``None``.

        """
        self.sock.setblocking(not nb)
        try:
            buffer = ''
            partial = ''
            while not buffer.endswith('\x00'):
                try:
                    partial = self.sock.recv(1)
                    if not partial or partial == '':
                        raise UnknownBrokerResponseError('empty reply')
                except socket.error, exc:
                    if exc[0] == EAGAIN or exc[0] == EWOULDBLOCK:
                        if not buffer or buffer == '\n':
                            raise UnknownBrokerResponseError('empty reply')
                        continue
                buffer += partial
        finally:
            self.sock.setblocking(nb)

        # ** Nasty Alert **
        # There may be a left over newline
        # RabbitMQ doesn't have a newline after \x00
        # ActiveMQ does.  This is a hack around that.
        # http://stomp.codehaus.org/Protocol mentions
        # nothing about a newline following the NULL (^@)
        if buffer[:1] == '\n':
            return buffer[1:-1]

        return buffer[:-1]

    def __repr__(self):
        return "<Frame %s>" % pformat(self.headers)
