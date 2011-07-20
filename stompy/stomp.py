import socket
from stompy.frame import Frame
from functools import wraps


class NotConnectedError(Exception):
    """No longer connected to the STOMP server."""


class ConnectionError(socket.error):
    """Couldn't connect to the STOMP server."""


class ConnectionTimeoutError(socket.timeout):
    """Timed-out while establishing connection to the STOMP server."""


class Stomp(object):
    """STOMP Client.

    :param hostname: Hostname of the STOMP server to connect to.
    :param port: The port to use. (default ``61613``)

    """
    ConnectionError = ConnectionError
    ConnectionTimeoutError = ConnectionTimeoutError
    NotConnectedError = NotConnectedError

    def __init__(self, hostname, port=61613):
        self.host = hostname
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._subscribed_to = {}
        self._subscribed = None
        self._callback = None
        self.connected = None
        self.frame = Frame()

    def connect(self, username=None, password=None, clientid=None):
        """Connect to STOMP server.

        :keyword username: Username for connection
        :keyword password: Password for connection
        :keyword clientid: Client identification for persistent connections
        """
        try:
            self.sock.connect((self.host, self.port))
            self.frame.connect(self.sock, username=username, password=password, clientid=clientid)
        except socket.timeout, exc:
            raise self.ConnectionTimeoutError(*exc.args)
        except socket.error, exc:
            raise self.ConnectionError(*exc.args)
        self.connected = True

    def disconnect(self, conf=None):
        """Disconnect from the server."""
        try:
            for destination in self._subscribed_to.keys():
                self.unsubscribe({"destination": destination})
            self._send_command("DISCONNECT", conf)
        except self.NotConnectedError:
            pass
        try:
            self.sock.shutdown(0)
            self.sock.close()
        except socket.error, exc:
            # likely wasn't connected
            pass
        self.connected = False

    def send(self, conf=None):
        """Send message to STOMP server

        You'll need to pass the body and any other headers your
        STOMP server likes.

        destination is **required**

        In the case of ActiveMQ with persistence, you could do this:

            >>> for i in xrange(1,1000):
            ...     stomp.send({'destination': '/queue/foo',
            ...                 'body': 'Testing',
            ...                 'persistent': 'true'})

        """
        headers = dict(conf)
        body = headers.pop("body", "")
        return self._send_command("SEND", headers, extra={"body": body},
                                  want_receipt=True)

    def _build_frame(self, *args, **kwargs):
        self._connected_or_raise()
        return self.frame.build_frame(*args, **kwargs)

    def subscribe(self, conf=None):
        """Subscribe to a given destination

        You will need to pass any headers your STOMP server likes.

        destination is *required*

        In the case of ActiveMQ, you could do this:

            >>> stomp.subscribe({'destination':'/queue/foo',
            ...                  'ack':'client'})
        """
        destination = conf["destination"]
        self._send_command("SUBSCRIBE", conf)
        self._subscribed_to[destination] = True

    def begin(self, conf=None):
        """Begin transaction.

        You will need to pass any headers your STOMP server likes.

        destination is *required*

        In the case of ActiveMQ, you could do this:

            >>> stomp.begin({'transaction':'<randomish_hash_like_thing>'})
        """
        self._send_command("BEGIN", conf)

    def commit(self, conf=None):
        """Commit transaction.

        You will need to pass any headers your STOMP server likes.

        destination is **required**

        In the case of ActiveMQ, you could do this:

            >>> stomp.commit({'transaction':'<randomish_hash_like_thing>'})

        """
        self._send_command("COMMIT", conf)

    def abort(self, conf=None):
        """Abort transaction.

        In the case of ActiveMQ, you could do this:

            >>> stomp.abort({'transaction':'<randomish_hash_like_thing>'})

        """
        self._send_command("ABORT", conf)

    def unsubscribe(self, conf=None):
        """Unsubscribe from a given destination

        You will need to pass any headers your STOMP server likes.

        destination is *required*

        >>> stomp.unsubscribe({'destination':'/queue/foo'})
        """
        destination = conf["destination"]
        self._send_command("UNSUBSCRIBE", conf)
        self._subscribed_to.pop(destination, None)

    def ack(self, frame):
        """Acknowledge receipt of a message

        :param: A :class:`stompy.frame.Frame` instance.

        Example

            >>> while True:
            ...     frame = stomp.receive_frame()
            ...     stomp.ack(frame)

        """
        message_id = frame.headers.get('message-id')
        self._send_command("ACK", {"message-id": message_id})

    def receive_frame(self, callback=None, nonblocking=False):
        """Get a frame from the STOMP server

        :keyword nonblocking: By default this function waits forever
            until there is a message to be received, however, in non-blocking
            mode it returns ``None`` if there is currently no message
            available.

        :keyword callback: Optional function to execute when message recieved.

        Note that you must be subscribed to one or more destinations.
        Use :meth:`subscribe` to subscribe to a topic/queue.

        Example: Blocking

            >>> while True:
            ...     frame = stomp.receive_frame()
            ...     print(frame.headers['message-id'])
            ...     stomp.ack(frame)

        Example: Non-blocking

            >>> frame = stomp.recieve_frame(nonblocking=True)
            >>> if frame:
            ...     process_message(frame)
            ... else:
            ...     # no messages yet.

        """
        self._connected_or_raise()
        self._callback = callback
        message = None
        if self._callback:
            message = self.frame.get_message(nb=nonblocking)
            self._callback(message)
            return
        else:
            return self.frame.get_message(nb=nonblocking)

    def poll(self, callback=None):
        """Alias to :meth:`receive_frame` with ``nonblocking=True``."""
        return self.receive_frame(nonblocking=True, callback=callback)

    def send_frame(self, frame):
        """Send a custom frame to the STOMP server

        :param frame: A :class:`stompy.frame.Frame` instance.

        Example

            >>> from stompy import Frame
            >>> frame = Frame().build_frame({
            ...    "command": "DISCONNECT",
            ...    "headers": {},
            ... })
            >>> stomp.send_frame(frame)

        """
        self._connected_or_raise()
        frame = self.frame.send_frame(frame.as_string())
        return frame

    def _send_command(self, command, conf=None, extra=None, **kwargs):
        conf = conf or {}
        extra = extra or {}
        frame_conf = {"command": command, "headers": conf}
        frame_conf.update(extra)
        frame = self._build_frame(frame_conf, **kwargs)
        reply = self.send_frame(frame)
        if kwargs.get("want_receipt", False):
            return reply
        return frame

    def _connected_or_raise(self):
        if not self.connected:
            raise self.NotConnectedError("Not connected to STOMP broker.")

    @property
    def subscribed(self):
        """**DEPRECATED** The queue or topic currently subscribed to."""
        as_list = self._subscribed_to.keys()
        if not as_list:
            return
        return as_list[0]
