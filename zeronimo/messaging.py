# -*- coding: utf-8 -*-
"""
   zeronimo.messaging
   ~~~~~~~~~~~~~~~~~~

   :copyright: (c) 2013-2016 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from collections import namedtuple
try:
    import cPickle as pickle
except ImportError:
    import pickle

from zeronimo.helpers import eintr_retry_zmq, make_repr


__all__ = ['ACK', 'DONE', 'ITER', 'ACCEPT', 'REJECT', 'RETURN', 'RAISE',
           'YIELD', 'BREAK', 'PACK', 'UNPACK', 'Call', 'Reply', 'send', 'recv']


# Method masks:
ACK = 0b10000000
DONE = 0b01000000
ITER = 0b00100000

# Methods:
ACCEPT = ACK | 0b01
REJECT = ACK | 0b10
RETURN = DONE | 0b01
RAISE = DONE | 0b10
YIELD = ITER | 0b01
BREAK = ITER | DONE | 0b10

#: The seam between topics (prefixes) and header-payload.
SEAM = '\xff'


#: The default function to pack message.
PACK = lambda obj: pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)

#: The default function to unpack message.
UNPACK = pickle.loads


class Call(namedtuple('Call', 'name call_id reply_to hints')):

    __repr__ = lambda x: make_repr(x, keywords=x._fields)


class Reply(namedtuple('Reply', 'method call_id task_id')):

    def __repr__(self):
        method = {
            ACCEPT: 'ACCEPT',
            REJECT: 'REJECT',
            RETURN: 'RETURN',
            RAISE: 'RAISE',
            YIELD: 'YIELD',
            BREAK: 'BREAK',
        }[self.method]
        class M(object):
            def __repr__(self):
                return method
        return make_repr(self, keywords=self._fields, data={'method': M()})


def send(socket, header, payload, topics=(), flags=0):
    """Sends header, payload, and topics through a ZeroMQ socket.

    :param socket: a zmq socket.
    :param header: a list of byte strings which represent a message header.
    :param payload: the serialized byte string of a payload.
    :param topics: a chain of topics.
    :param flags: zmq flags to send messages.

    """
    msgs = []
    msgs.extend(topics)
    msgs.append(SEAM)
    msgs.extend(header)
    msgs.append(payload)
    return eintr_retry_zmq(socket.send_multipart, msgs, flags)


def recv(socket, flags=0, capture=(lambda msgs: None)):
    """Receives header, payload, and topics through a ZeroMQ socket.

    :param socket: a zmq socket.
    :param flags: zmq flags to receive messages.
    :param capture: a function to capture received messages.

    """
    msgs = eintr_retry_zmq(socket.recv_multipart, flags)
    capture(msgs)
    try:
        seam = msgs.index(SEAM)
    except ValueError:
        raise EOFError('no seam after topics')
    if seam == len(msgs) - 1:
        raise EOFError('neither header nor payload')
    topics = msgs[:seam]
    header = msgs[seam + 1:-1]
    payload = msgs[-1]
    return header, payload, topics
