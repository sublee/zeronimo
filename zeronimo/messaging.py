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

import zmq

from .helpers import eintr_retry_zmq, make_repr


__all__ = ['ACK', 'DONE', 'ITER', 'ACCEPT', 'REJECT', 'RETURN', 'RAISE',
           'YIELD', 'BREAK', 'PACK', 'UNPACK', 'Call', 'Reply', 'send', 'recv']


# method masks
ACK = 0b10000000
DONE = 0b01000000
ITER = 0b00100000

# methods
ACCEPT = ACK | 0b01
REJECT = ACK | 0b10
RETURN = DONE | 0b01
RAISE = DONE | 0b10
YIELD = ITER | 0b01
BREAK = ITER | DONE | 0b10

SEAM = '\xff'


#: The default function to pack message.
PACK = lambda obj: pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)

#: The default function to unpack message.
UNPACK = pickle.loads


class Call(namedtuple('Call', 'name call_id reply_to')):

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


def send(socket, header, payload, prefixes=(), flags=0):
    """Sends prefixes, header, payload through a ZeroMQ socket.

    :param socket: a zmq socket.
    :param header: a list of byte strings which represent a message header.
    :param payload: the serialized byte string of a payload.
    :param prefixes: a chain of prefixes.

    """
    msgs = []
    msgs.extend(prefixes)
    msgs.append(SEAM)
    msgs.extend(header)
    msgs.append(payload)
    return eintr_retry_zmq(socket.send_multipart, msgs, flags)


def recv(socket, flags=0, capture=(lambda msgs: None)):
    """Receives prefixes, header, payload via a ZeroMQ socket."""
    prefixes = []
    while True:
        msg = eintr_retry_zmq(socket.recv, flags)
        capture([msg])
        if msg == SEAM:
            break
        elif not socket.getsockopt(zmq.RCVMORE):
            raise EOFError('no seam after prefixes')
        prefixes.append(msg)
    if not socket.getsockopt(zmq.RCVMORE):
        raise EOFError('neither header nor payload')
    msgs = eintr_retry_zmq(socket.recv_multipart, flags)
    capture(msgs)
    header, payload = msgs[:-1], msgs[-1]
    return header, payload, prefixes
