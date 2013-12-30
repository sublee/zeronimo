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


def make_repr(obj, params=None, keywords=None, data=None, name=None):
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
    if data is not None:
        get = lambda attr: data[attr] if attr in data else getattr(obj, attr)
    else:
        get = functools.partial(getattr, obj)
    if params is not None:
        opts.append(', '.join([repr(get(attr)) for attr in params]))
    if keywords is not None:
        opts.append(', '.join(
            ['{0}={1!r}'.format(attr, get(attr)) for attr in keywords]))
    if name is None:
        name = cls_name(obj)
    return '{0}({1})'.format(name, ', '.join(opts))
