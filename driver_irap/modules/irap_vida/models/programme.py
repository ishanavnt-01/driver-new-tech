# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .base_model import BaseModel


class Programme(BaseModel):

    attributes = ['id', 'name', 'manager_id']


class ProgrammeUser(BaseModel):

    attributes = ['id', 'user_id', 'access_level', 'user_manager', 'programme_id']
