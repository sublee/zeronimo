# -*- coding: utf-8 -*-
"""
ØRPC
~~~~

A distributed RPC solution based on ØMQ.

.. sourcecode:: python

   import socket
   import zeronimo

   class Application(object):

       @zeronimo.remote_method(fanout=True)
       def whoami(self):
           return socket.gethostname()

   server = zeronimo.Server(Application())
   client = zeronimo.Client()

   with client.link(server) as bridge:
       all_hostnames = list(bridge.whoami())

"""
from __future__ import with_statement
import distutils
import os
import re
from setuptools import setup
from setuptools.command.test import test
import sys


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
    description='A distributed RPC solution based on ØMQ',
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
    tests_require=['pytest'],
)
