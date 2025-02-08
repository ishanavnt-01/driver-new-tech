# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .base_model import BaseModel


class Bound(BaseModel):

    attributes = [
        'minLat', 'dataset_id', 'maxLon', 'minLon', 'maxLat'
    ]
