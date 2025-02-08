# -*- coding: utf-8 -*-
from __future__ import absolute_import


class BaseModel(object):

    attributes = NotImplementedError

    def __init__(self, data=None):
        if data:
            for attr in self.attributes:
                setattr(self, attr, data.get(attr, None))

    def __iter__(self):
        for attr in self.attributes:
            yield attr, getattr(self, attr)
