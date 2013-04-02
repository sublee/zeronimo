# -*- coding: utf-8 -*-
"""
Zeronimo
~~~~~~~~

A distributed RPC solution based on ZeroMQ_. Follow the features:

- A worker can return a value to the remote customer.
- A worker can yield a value to the remote customer.
- A worker can raise an error to the remote customer.
- A customer can invoke to any remote worker in the worker cluster.
- A customer can invoke to all remote workers in the worker cluster.

.. sourcecode:: python

   import socket
   import zeronimo

   class Application(object):

       @zeronimo.register(fanout=True)
       def whoami(self):
           # hostname
           yield socket.gethostname()
           # public address
           sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
           sock.connect(('8.8.8.8', 53))
           yield sock.getsockname()[0]

   worker = zeronimo.Worker(Application())
   customer = zeronimo.Customer()

   with customer.link([worker]) as tunnel:
       for result in tunnel.whoami():
           print 'hostname=', result.next()
           print 'public address=', result.next()

.. _ZeroMQ: http://www.zeromq.org/

"""
from __future__ import with_statement
import distutils
import os
import re
from setuptools import setup
from setuptools.command.test import test
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
    raise SystemExit(__import__('pytest').main([test_file]))
test.run_tests = run_tests


setup(
    name='zeronimo',
    version=version,
    license='BSD',
    author='Heungsub Lee',
    author_email=re.sub('((sub).)(.*)', r'\2@\1.\3', 'sublee'),
    url='http://github.com/sublee/zeronimo/',
    description='A distributed RPC solution based on ZeroMQ',
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
    install_requires=['distribute', 'gevent', 'pyzmq>=13'],
    test_suite='zeronimotests',
    tests_require=['pytest', 'decorator'],
)
