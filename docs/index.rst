Zeronimo
========

RPC between distributed nodes via ØMQ

.. currentmodule:: zeronimo

What's Zeronimo?
~~~~~~~~~~~~~~~~

Zeronimo provides ØMQ socket wrappers to use RPC within the ØMQ network. You
can request some remote worker or all workers to call some method. Make a
network using only ØMQ and wrap them with Zeronimo wrappers.

There are 3 wrappers:

- :class:`Worker` runs an RPC service over multiple sockets. PULL, SUB, PAIR
  type allowed.
- :class:`Customer` requests workers which are communicable with the customer's
  PUSH or PUB or PAIR socket to call some method.
- :class:`Collector` receives replies from the emitted worker via PULL socket
  or same socket with the customer's PAIR socket.

return/yield/raise
~~~~~~~~~~~~~~~~~~

Remote functions could not just return a value. It could also yield multiple
values and raise an exception as reply messages. When a :class:`Customer`
contains a :class:`Collector`, RPC call works like local function call.

.. sourcecode:: python

   collector = zeronimo.Collector(pull, 'tcp://127.0.0.1:8912')
   customer = zeronimo.Customer(push, collector)
   customer.emit_some_function()

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
