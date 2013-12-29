# -*- coding: utf-8 -*-
"""
Zeronimo
~~~~~~~~

A distributed RPC solution based on ØMQ_ and gevent_. Follow the features:

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
   worker_sock = ctx.socket(zmq.PULL)
   worker_sock.bind('tcp://*:24600')

   worker = zeronimo.Worker(Application(), [worker_sock])
   worker.run()

Client-side
-----------

The address is 192.168.0.42. The reply collector will listen at 24601.

.. sourcecode:: python

   import zmq.green as zmq
   import zeronimo

   ctx = zmq.Context()

   collector_sock = ctx.socket(zmq.PULL)
   collector_sock.bind('tcp://*:24601)
   collector = zeronimo.Collector(collector_sock, 'tcp://192.168.0.42:24601')

   customer_sock = ctx.socket(zmq.PUSH)
   customer_sock.connect('tcp://192.168.0.41:24600')
   customer = zeronimo.Customer(customer_sock, collector)

   for line in customer.rycbar123():
       print line

"""
from __future__ import with_statement
import distutils
import os
import re
from setuptools import setup
from setuptools.command.test import test
import subprocess
import sys
# prevent error in sys.exitfunc when testing
if 'test' in sys.argv: import zmq


# detect the current version
with open('zeronimo/__init__.py') as f:
    version = re.search(r'__version__\s*=\s*\'(.+?)\'', f.read()).group(1)
assert version


# use pytest instead
def run_tests(self):
    pyc = re.compile(r'\.pyc|\$py\.class')
    test_file = pyc.sub('.py', __import__(self.test_suite).__file__)
    test_args = ['py.test', test_file, '--inproc', '--tcp']
    raise SystemExit(subprocess.call(test_args))
test.run_tests = run_tests


setup(
    name='zeronimo',
    version=version,
    license='BSD',
    author='Heungsub Lee',
    author_email=re.sub('((sub).)(.*)', r'\2@\1.\3', 'sublee'),
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
    test_suite='zeronimotests',
    tests_require=['pytest', 'decorator', 'psutil'],
)
