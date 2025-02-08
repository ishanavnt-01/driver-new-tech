# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .base_model import BaseModel


class Region(BaseModel):

    attributes = [
        'id', 'programme_id', 'name', 'manager_id'
    ]


class RegionUser(BaseModel):

    attributes = ['id', 'user_id', 'access_level', 'user_manager', 'region_id']


class RegionProgramme(BaseModel):

    attributes = ['id', 'programme_id', 'name', 'manager_id']
