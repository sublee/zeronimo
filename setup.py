# -*- coding: utf-8 -*-
"""
Zeronimo
~~~~~~~~

A distributed RPC solution based on ØMQ_ and gevent_.  Follow the features:

- A worker can return, yield, raise any picklable object to the remote
  customer.
- A customer can invoke to any remote worker in the worker cluster.
- A customer can invoke to all remote workers in the worker cluster.

.. _ØMQ: http://www.zeromq.org/
.. _gevent: http://gevent.org/

Example
=======

Server-side
-----------

The address is 192.168.0.41. The worker will listen at 24600.

.. sourcecode:: python

   import zmq.green as zmq
   import zeronimo

   class Application(object):

       def rycbar123(self):
           for word in 'run, you clever boy; and remember.'.split():
               yield word

   ctx = zmq.Context()

   # make worker
   worker_sock = ctx.socket(zmq.PULL)
   worker_sock.bind('tcp://*:24600')
   worker = zeronimo.Worker(Application(), [worker_sock])

   # run worker forever
   worker.run()

Client-side
-----------

The address is 192.168.0.42. The reply collector will listen at 24601.

.. sourcecode:: python

   import zmq.green as zmq
   import zeronimo

   ctx = zmq.Context()

   # make remote result collector
   collector_sock = ctx.socket(zmq.PULL)
   collector_sock.bind('tcp://*:24601)
   collector = zeronimo.Collector(collector_sock, 'tcp://192.168.0.42:24601')

   # make customer
   customer_sock = ctx.socket(zmq.PUSH)
   customer_sock.connect('tcp://192.168.0.41:24600')
   customer = zeronimo.Customer(customer_sock, collector)

   # rpc
   remote_result = customer.emit('rycbar123')
   for line in remote_result.get():
       print line

"""
from __future__ import with_statement

import os
import sys

from setuptools import setup
from setuptools.command.test import test


# prevent error in sys.exitfunc when testing.
__import__('zmq') if 'test' in sys.argv else None


# include __about__.py.
__dir__ = os.path.dirname(__file__)
about = {}
with open(os.path.join(__dir__, 'zeronimo', '__about__.py')) as f:
    exec(f.read(), about)


# use pytest instead.
def run_tests(self):
    raise SystemExit(__import__('pytest').main(['-v', '--all']))
test.run_tests = run_tests


setup(
    name='zeronimo',
    version=about['__version__'],
    license=about['__license__'],
    author=about['__author__'],
    author_email=about['__author_email__'],
    url='http://github.com/sublee/zeronimo/',
    description='RPC between distributed workers',
    long_description=__doc__,
    platforms='any',
    packages=['zeronimo'],
    classifiers=['Development Status :: 1 - Planning',
                 'Intended Audience :: Developers',
                 'License :: OSI Approved :: BSD License',
                 'Operating System :: OS Independent',
                 'Programming Language :: Python',
                 'Programming Language :: Python :: 2',
                 'Programming Language :: Python :: 2.7',
                 'Programming Language :: Python :: Implementation :: CPython',
                 'Topic :: Software Development'],
    install_requires=['gevent>=1', 'pyzmq>=14'],
    test_suite='...',
    tests_require=['pytest', 'pytest-rerunfailures',
                   'psutil', 'pylru', 'singledispatch'],
)
