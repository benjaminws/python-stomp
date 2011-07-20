from stompy.stomp import Stomp
from Queue import Empty
from uuid import uuid4


class TransactionError(Exception):
    """Transaction related error."""


class Client(object):
    """Simple STOMP client.

    :keyword host: Hostname of the server to connect to (default:
        ``localhost``)
    :keyword port: Port of the server to connect to (default: ``61613``)

    Example

        >>> from stompy.simple import Client
        >>> stomp = Client()
        >>> stomp.connect()
        >>> stomp.put("The quick brown fox...", destination="/queue/test")
        >>> stomp.subscribe("/queue/test")
        >>> message = stomp.get_nowait()
        >>> message.body
        'The quick brown fox...'
        >>> stomp.ack(message)
        >>> stomp.unsubscribe("/queue/test")
        >>> stomp.disconnect()

    """
    Empty = Empty

    def __init__(self, host="localhost", port=61613):
        self.stomp = Stomp(host, port)
        self._current_transaction = None

    def get(self, block=True, callback=None):
        """Get message.

        :keyword block: Block if necessary until an item is available.
            If this is ``False``, return an item if one is immediately
            available, else raise the :exc:`Empty` exception.

        :keyword callback: Optional function to execute when message recieved.

        :raises Empty: If ``block`` is off and no message was receied.

        """
        frame = self.stomp.receive_frame(nonblocking=not block, callback=callback)
        if frame is None and not block:
            raise self.Empty()
        return frame

    def get_nowait(self):
        """Remove and return an item from the queue without blocking.

        Only get an item if one is immediately available. Otherwise
        raise the :exc:`Empty` exception.

        See :meth:`get`.

        """
        return self.get(block=False)

    def put(self, item, destination, persistent=True, conf=None):
        """Put an item into the queue.

        :param item: Body of the message.
        :param destination: Destination queue.
        :keyword persistent: Is message persistent? (store on disk).
        :keyword conf: Extra headers to send to the broker.

        :returns: The resulting :class:`stompy.frame.Frame` instance.

        """
        persistent = "true" if persistent else "false"
        conf = self._make_conf(conf, body=item, destination=destination,
                               persistent=persistent)

        return self.stomp.send(conf)

    def connect(self, username=None, password=None, clientid=None):
        """Connect to the broker.

        :keyword username: Username for connection
        :keyword password: Password for connection
        :keyword clientid: Client identification for persistent connections

        :raises :exc:`stompy.stomp.ConnectionError`:
            if the connection was unsuccessful.
        :raises :exc:`stompy.stomp.ConnectionTimeoutError`:
            if the connection timed out.

        """
        self.stomp.connect(username=username, password=password, clientid=clientid)

    def disconnect(self):
        """Disconnect from the broker."""
        self.stomp.disconnect()

    def subscribe(self, destination, ack="auto", conf=None):
        """Subscribe to topic/queue.

        :param destination: The destination queue/topic to subscribe to.
        :keyword ack: How to handle acknowledgment, either
            ``auto`` - ack is handled by the server automatically, or
            ``client`` - ack is handled by the client itself by calling
            :meth:`ack`.
        :keyword conf: Additional headers to send with the subscribe request.

        """
        conf = self._make_conf(conf, destination=destination, ack=ack)
        return self.stomp.subscribe(conf)

    def unsubscribe(self, destination, conf=None):
        """Unsubscribe from topic/queue previously subscribed to.

        :param destination: The destination queue/topic to unsubscribe from.
        :keyword conf: Additional headers to send with the unsubscribe
            request.

        """
        conf = self._make_conf(conf, destination=destination)
        return self.stomp.unsubscribe(conf)

    def begin(self, transaction):
        """Begin transaction.

        Every :meth:`ack` and :meth:`send` will be affected by this
        transaction and won't be real until a :meth:`commit` is issued.
        To roll-back any changes since the transaction started use
        :meth:`abort`.

        """
        if self._current_transaction:
            raise TransactionError(
                "Already in transaction. Please commit or abort first!")
        self._current_transaction = str(uuid4())
        return self.stomp.begin({"transaction": self._current_transaction})

    def commit(self, transaction):
        """Commit current transaction."""
        if not self._current_transaction:
            raise TransactionError("Not in transaction")
        self.stomp.commit({"transaction": self._current_transaction})
        self._current_transaction = None

    def abort(self):
        """Roll-back current transaction."""
        if not self._current_transaction:
            raise TransactionError("Not in transaction")
        self.stomp.abort({"transaction": self._current_transaction})
        self._current_transaction = None

    def ack(self, frame):
        """Acknowledge message.

        :param frame: The message to acknowledge.

        """
        return self.stomp.ack(frame)

    def _make_conf(self, conf, **kwargs):
        kwargs.update(dict(conf or {}))
        if self._current_transaction:
            conf["transaction"] = self._current_transaction
        return kwargs
