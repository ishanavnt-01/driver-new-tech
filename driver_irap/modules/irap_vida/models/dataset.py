# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .base_model import BaseModel


class Dataset(BaseModel):

    attributes = [
        'id', 'name', 'project_id', 'variables_id',
        'status_id', 'manager_id', 'total_length', 'is_processing',
        'last_process', 'has_data', 'unix_timestamp',
        'description', 'type_id'
    ]


class DatasetUser(BaseModel):

    attributes = [
        'id', 'user_id', 'dataset_id', 'access_level',
        'user_manager'
    ]
