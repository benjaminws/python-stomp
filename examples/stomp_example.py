#!/usr/bin/env python
import sys
import time
from stompy import Stomp
from optparse import OptionParser


def consume(host, port, queue, num=None):
    try:
        stomp = Stomp(host, port)
        # optional connect keyword args "username" and "password" like so:
        # stomp.connect(username="user", password="pass")
        stomp.connect()
    except:
        print("Cannot connect")
        raise

    # If using RabbitMQ, the queue seems to 'disappear' 
    # after disconnecting a consumer, to make the queue persistent
    # add the headers 'auto-delete': 'false' and 'durable': 'true'
    # to the dictionary below
    stomp.subscribe({'destination': queue, 'ack': 'client'})

    if not num:
        while True:
            try:
                frame = stomp.receive_frame()
                stomp.ack(frame)
                print(frame.headers.get('message-id'))
                print(frame.body)
            except KeyboardInterrupt:
                stomp.disconnect()
                break
    else:
        for i in xrange(0, num):
            try:
                frame = stomp.receive_frame()
                stomp.ack(frame)
                print(frame.headers.get('message-id'))
                print(frame.body)
            except KeyboardInterrupt:
                stomp.disconnect()
                break
        stomp.disconnect()


def produce(host, port, queue, num=1000):
    try:
        stomp = Stomp(host, port)
        # optional connect keyword args "username" and "password" like so:
        # stomp.connect(username="user", password="pass")
        stomp.connect()
    except:
        print("Cannot connect")
        raise

    for i in xrange(0, num):
        print("Message #%d" % i)
        this_frame = stomp.send({'destination': queue,
                                 'body': 'Testing %d' % i,
                                 'persistent': 'true'})
        print("Receipt: %s" % this_frame.headers.get('receipt-id'))

    stomp.disconnect()

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-H', '--host', action='store',
                      type='string', dest='host', help='hostname')
    parser.add_option('-p', '--port', action='store',
                      type='int', dest='port', help='port')
    parser.add_option('-q', '--queue', action='store',
                      type='string', dest='queue', help='destination queue')
    parser.add_option('-P', '--produce', action='store_true',
                      default=False, dest='produce', help='produce messages')
    parser.add_option('-c', '--consume', action='store_true',
                      default=False, dest='consume', help='consume messages')
    parser.add_option('-n', '--number', action='store',
                      type='int', dest='number',
                      help='produce or consume NUMBER messages')

    options, args = parser.parse_args()

    if not options.host:
        print("Host name is required!")
        parser.print_help()
        sys.exit(1)
    if not options.port:
        print("Port is required!")
        parser.print_help()
        sys.exit(1)
    if not options.queue:
        print("Queue name is required!")
        parser.print_help()
        sys.exit(1)

    if options.produce:
        produce(options.host, options.port, options.queue, options.number)
    elif options.consume:
        consume(options.host, options.port, options.queue, options.number)
