# -*- coding: utf-8 -*-
"""
   zeronimo.exceptions
   ~~~~~~~~~~~~~~~~~~~

   :copyright: (c) 2013-2015 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""


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

    pass
