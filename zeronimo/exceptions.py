# -*- coding: utf-8 -*-
"""
   zeronimo.exceptions
   ~~~~~~~~~~~~~~~~~~~

   :copyright: (c) 2013-2016 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from contextlib import contextmanager
import sys


__all__ = [
    'ZeronimoException', 'EmissionError', 'TaskError', 'WorkerNotFound',
    'Rejected', 'Undelivered', 'TaskClosed', 'MalformedMessage']


class ZeronimoException(BaseException):
    """The base exception for exceptions Zeronimo customized."""

    pass


class EmissionError(ZeronimoException):

    pass


class TaskError(ZeronimoException):

    pass


class WorkerNotFound(EmissionError):

    pass


class Rejected(EmissionError):

    pass


class Undelivered(EmissionError):

    pass


class TaskClosed(TaskError):

    pass


class MalformedMessage(ZeronimoException, RuntimeWarning):
    """Warns when a received message is not expected format."""

    exception = None
    message_parts = ()

    def __init__(self, *args):
        num_args = len(args)
        if num_args == 1:
            # (errmsg)
            super(MalformedMessage, self).__init__(args[0])
        elif num_args == 2:
            # (exception, message_parts)
            self.exception, self.message_parts = args
        else:
            raise TypeError('pass 1 or 2 arguments')

    @classmethod
    @contextmanager
    def wrap(cls, message_parts):
        """Wraps exceptions in the context with :exc:`MalformedMessage`."""
        try:
            yield
        except BaseException as exception:
            __, __, tb = sys.exc_info()
            raise cls, cls(exception, message_parts), tb
