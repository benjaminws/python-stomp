==========================================
 stompy - Python STOMP client library
==========================================

This is useful for connecting to and communicating with
Apache `ActiveMQ`_ (an open source `Java Message Service`_ (JMS)
message broker) or other brokers with support for the `STOMP`_ protocol.

The majority of the methods available take a single argument; a dictionary.
This dictionary should contain the necessary bits you need
to pass to the `STOMP`_ server.  It is outlined in each method
exactly what it needs to work.

For specifics on the protocol, see the `STOMP protocol specification`_.

This library is basically a Python implementation of Perl's `Net::Stomp`_.

To enable the `ActiveMQ`_ Broker for `STOMP`_ add the following to the
``activemq.xml`` configuration::

    <connector>
        <serverTransport uri="stomp://localhost:61613"/>
    </connector>

See http://bitbucket.org/benjaminws/python-stomp/ for latest code.

.. _`ActiveMQ`: http://activemq.apache.org/
.. _`Java Message Service`: http://java.sun.com/products/jms/
.. _`STOMP`: http://stomp.codehaus.org/
.. _`STOMP protocol specification`: http://stomp.codehaus.org/Protocol
.. _`Net::Stomp`: http://search.cpan.org/perldoc?Net::Stomp

Thanks for patches and support go out to:

Ask Solem Hoel (asksol) http://github.com/ask
Victor Ng (crankycoder) http://crankycoder.com/
Justin Azoff (justinazoff) http://www.bouncybouncy.net/
