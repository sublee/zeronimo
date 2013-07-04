# -*- coding: utf-8 -*-
"""
    zeronimo.helpers
    ~~~~~~~~~~~~~~~~

    Helper functions.

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""


__all__ = ['cls_name', 'make_repr']


def cls_name(obj):
    """Returns the class name of the object."""
    return type(obj).__name__


def make_repr(obj, params=[], keywords=[], data={}):
    """Generates a string of object initialization code style. It is useful
    for custom __repr__ methods.

        class Example(object):

            def __init__(self, param, keyword=None):
                self.param = param
                self.keyword = keyword

            def __repr__(self):
                return make_repr(self, ['param'], ['keyword'])

        >>> Example('hello', keyword='world')
        Example('hello', keyword='world')
    """
    get = lambda attr: data[attr] if attr in data else getattr(obj, attr)
    opts = []
    if params:
        opts.append(', '.join([repr(get(attr)) for attr in params]))
    if keywords:
        opts.append(', '.join(
            ['{0}={1!r}'.format(attr, get(attr)) for attr in keywords]))
    return '{0}({1})'.format(cls_name(obj), ', '.join(opts))
