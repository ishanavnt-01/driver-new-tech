# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .base_model import BaseModel


class User(BaseModel):

    attributes = [
        'id', 'name', 'first_name', 'active',
        'last_name', 'email', 'title', 'account_type',
        'organisation', 'position_title', 'country',
        'delimiter', 'decimal_mark'
    ]