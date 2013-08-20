Zeronimo
========

RPC between distributed nodes via ØMQ

.. currentmodule:: zeronimo

Overview
~~~~~~~~

Maybe you don't use a single node.

Many RPC systems provide simple server-client model because they were stacked
over TCP connection. Zeronimo is an RPC system which wraps ØMQ sockets so you
can make more complex node distribution.

Patterns
~~~~~~~~

Zeronimo provides various emission options. So you can choose a pattern not
only simple client-server communication model.

.. figure:: emit.png
   :align: center

   Emit to one worker.

API
~~~

.. automodule:: zeronimo
   :members:
