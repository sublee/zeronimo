# -*- coding: utf-8 -*-
"""
    zeronimo.functional
    ~~~~~~~~~~~~~~~~~~~

    Provides higher-order functions.

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
from collections import Iterable, Sequence, Set, Mapping, namedtuple
import functools
import hashlib

from gevent.coros import Semaphore


Spec = namedtuple('Spec', ['func', 'fanout', 'reply'])


def register(f=None, fanout=False, reply=True):
    """This decorator makes a function to be collected by
    :func:`collect_blueprint` for being invokable by remote clients.
    """
    def decorator(f):
        f._znm_fanout = fanout
        f._znm_reply = reply
        return f
    return decorator(f) if f is not None else decorator


def extract_blueprint(obj):
    """Collects remote functions from the object."""
    blueprint = {}
    for attr in dir(obj):
        func = getattr(obj, attr)
        try:
            fanout, reply = func._znm_fanout, func._znm_reply
        except AttributeError:
            continue
        blueprint[func.__name__] = Spec(func, fanout, reply)
    return blueprint


def make_fingerprint(blueprint):
    hexh = lambda x: hex(hash(x))
    md5, sha1 = hashlib.md5(), hashlib.sha1()
    for fn, spec in blueprint.viewitems():
        frag = ' '.join(map(repr, [fn, spec.fanout, spec.reply])) + '\n'
        md5.update(frag)
        sha1.update(frag)
    return '-'.join([md5.hexdigest(), sha1.hexdigest()])


def should_yield(val):
    return (
        isinstance(val, Iterable) and
        not isinstance(val, (Sequence, Set, Mapping)))
