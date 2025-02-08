# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .base_model import BaseModel


class Location(BaseModel):
    attributes = [
        'distance', 'road_name', 'section', 'carriageway', 'longitude',
        'latitude_to', 'length', 'smoothed_section_id', 'latitude', 'dataset_id',
        'location_id', 'longitude_to',
    ]


class LocationProgramme(BaseModel):
    # Same for programme, project, dataset, region

    attributes = [
        'distance', 'road_name', 'section', 'carriageway', 'longitude',
        'latitude_to', 'length', 'smoothed_section_id', 'latitude', 'dataset_id',
        'location_id', 'longitude_to',
    ]
