# -*- coding: utf-8 -*-
"""
    zeronimo.exceptions
    ~~~~~~~~~~~~~~~~~~~

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
import gevent


class ZeronimoError(RuntimeError): pass
class NoWorkerError(ZeronimoError): pass

#: an alias for :exc:`gevent.Timeout`.
TimeoutError = gevent.Timeout
