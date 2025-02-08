# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .base_model import BaseModel


class MapStarRating(BaseModel):

    attributes = [
        'dataset_id',
        'location_id',
        'latitude',
        'longitude',
        'latitude_to',
        'longitude_to',
        'bicycle_star_before',
        'bicycle_smoothed_star_before',
        'car_star_before',
        'car_smoothed_star_before',
        'motorcycle_star_before',
        'motorcycle_smoothed_star_before',
        'pedestrian_star_before',
        'pedestrian_smoothed_star_before',
        'bicycle_star_after',
        'bicycle_smoothed_star_after',
        'car_star_after',
        'car_smoothed_star_after',
        'motorcycle_star_after',
        'motorcycle_smoothed_star_after',
        'pedestrian_star_after',
        'pedestrian_smoothed_star_after'
    ]

