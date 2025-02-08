# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .base_model import BaseModel


class Project(BaseModel):

    attributes = ['country_id', 'id', 'manager_id', 'model_id', 'name', 'region_id']


class ProjectUser(BaseModel):

    attributes = ['id', 'user_id', 'access_level', 'user_manager', 'project_id']


class ProjectProgramme(BaseModel):

    attributes = ['id', 'region_id', 'name', 'model_id', 'manager_id', 'country_id']


class ProjectRegion(BaseModel):

    attributes = ['id', 'region_id', 'name', 'model_id', 'manager_id', 'country_id']
