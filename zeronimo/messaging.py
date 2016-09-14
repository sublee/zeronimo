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
ACCEPT = chr(ACK | 0b01)
REJECT = chr(ACK | 0b10)
RETURN = chr(DONE | 0b01)
RAISE = chr(DONE | 0b10)
YIELD = chr(ITER | 0b01)
BREAK = chr(ITER | DONE | 0b10)

SEAM = '\xff'


#: The default function to pack message.
PACK = lambda obj: pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)

#: The default function to unpack message.
UNPACK = pickle.loads


repr_namedtuple = lambda x: make_repr(x, keywords=x._fields)


class Call(namedtuple('Call', 'name call_id reply_to')):

    __repr__ = repr_namedtuple


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
    """Sends a Python object via a ZeroMQ socket. It also can append PUB/SUB
    prefix.

    :param socket: a zmq socket.
    :param header: a list of byte strings which represent a message header.
    :param payload: the serialized byte string of a payload.

    """
    parts = []
    parts.extend(prefixes)
    parts.append(SEAM)
    parts.extend(header)
    parts.append(payload)
    return eintr_retry_zmq(socket.send_multipart, parts, flags)


def recv(socket, flags=0):
    """Receives a Python object via a ZeroMQ socket."""
    prefixes = []
    while True:
        msg = eintr_retry_zmq(socket.recv, flags)
        if msg == SEAM:
            break
        elif not socket.getsockopt(zmq.RCVMORE):
            raise EOFError('seam after prefixes not received')
        prefixes.append(msg)
    if not socket.getsockopt(zmq.RCVMORE):
        raise EOFError('too few message parts')
    parts = eintr_retry_zmq(socket.recv_multipart, flags)
    header, payload = parts[:-1], parts[-1]
    return prefixes, header, payload
