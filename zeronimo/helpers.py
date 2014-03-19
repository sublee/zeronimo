# -*- coding: utf-8 -*-
"""
    zeronimo.helpers
    ~~~~~~~~~~~~~~~~

    Helper functions.

    :copyright: (c) 2013-2014 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
import functools


__all__ = ['cls_name', 'make_repr']


def cls_name(obj):
    """Returns the class name of the object."""
    return type(obj).__name__


def _repr_attr(obj, attr, data=None, reprs=None):
    val = getattr(obj, attr)
    if data is not None:
        val = data.get(attr, val)
    if reprs is None:
        repr_f = repr
    else:
        repr_f = reprs.get(attr, repr)
    return repr_f(val)


def make_repr(obj, params=None, keywords=None, data=None, name=None,
              reprs=None):
    """Generates a string of object initialization code style. It is useful
    for custom __repr__ methods::

       class Example(object):

           def __init__(self, param, keyword=None):
               self.param = param
               self.keyword = keyword

           def __repr__(self):
               return make_repr(self, ['param'], ['keyword'])

    See the representation of example object::

       >>> Example('hello', keyword='world')
       Example('hello', keyword='world')
    """
    opts = []
    if params is not None:
        opts.append(', '.join(
            _repr_attr(obj, attr, data, reprs) for attr in params))
    if keywords is not None:
        opts.append(', '.join(
            '{0}={1}'.format(attr, _repr_attr(obj, attr, data, reprs))
            for attr in keywords))
    if name is None:
        name = cls_name(obj)
    return '{0}({1})'.format(name, ', '.join(opts))
