#!/usr/bin/env python
import sys
import itertools
import time
from stompy import Stomp
from optparse import OptionParser

def callback_func(message):
    print("Using Callback Function")
    print(message.headers.get('message-id'))
    print(message.body)
    stomp.ack(message)

def consume(queue, num=None, callback=None):

    # If using RabbitMQ, the queue seems to 'disappear' 
    # after disconnecting a consumer, to make the queue persistent
    # add the headers 'auto-delete': 'false' and 'durable': 'true'
    # to the dictionary below
    stomp.subscribe({'destination': queue, 'ack': 'client'})

    def _handle_message(frame):
        print(frame.headers.get('message-id'))
        print(frame.body)
        stomp.ack(frame)

    # if num is not set, iterate forever.
    it = xrange(0, num) if num else itertools.count()

    try:
        for i in it:
            if callback:
                stomp.receive_frame(callback=callback_func)
            else:
                frame = stomp.receive_frame()
                _handle_message(frame)
    finally:
        stomp.disconnect()


def produce(queue, num=1000):

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
    parser.add_option('-C', '--use-callback', action='store_true',
                      default=False, dest='callback',
                      help='send retrieved message to python callable')
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

    try:
        stomp = Stomp(options.host, options.port)
        # optional connect keyword args "username" and "password" like so:
        # stomp.connect(username="user", password="pass")
        stomp.connect()
    except:
        print("Cannot connect")
        raise

    if options.produce:
        produce(options.queue, options.number)
    elif options.consume:
        consume(options.queue, options.number, options.callback)
