# -*- coding: utf-8 -*-
"""
   zeronimo.messaging
   ~~~~~~~~~~~~~~~~~~

   :copyright: (c) 2013-2015 by Heungsub Lee
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


#: The default function to pack message.
PACK = lambda obj: pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)

#: The default function to unpack message.
UNPACK = pickle.loads


class Call(namedtuple('Call', 'funcname args kwargs call_id reply_to')):

    def __repr__(self):
        return make_repr(self, keywords=self._fields)


class Reply(namedtuple('Reply', 'method data call_id task_id')):

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


def send(socket, obj, flags=0, topic=None, pack=PACK):
    """Sends a Python object via a ZeroMQ socket. It also can append PUB/SUB
    topic.
    """
    msg = pack(obj)
    if topic:
        eintr_retry_zmq(socket.send, topic, flags | zmq.SNDMORE)
    return eintr_retry_zmq(socket.send, msg, flags)


def recv(socket, flags=0, unpack=UNPACK):
    """Receives a Python object via a ZeroMQ socket."""
    msg = eintr_retry_zmq(socket.recv_multipart, flags)[-1]
    try:
        return unpack(msg)
    except BaseException as exc:
        exc._zeronimo_message = msg  # temporarily.
        raise
