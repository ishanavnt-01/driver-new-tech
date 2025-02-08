# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .base_model import BaseModel


class Fatality(BaseModel):

    attributes = [
        'vru_fatalities',
        'bicycle_fe',
        'mc_fe_run_off_loc_passenger_side',
        'mc_fe_along',
        'car_fe_intersection',
        'mc_fe_intersection',
        'mc_fe_property_access',
        'ksi_per_year',
        'mc_fe_head_on_overtaking',
        'bicycle_fe_intersection',
        'vehicle_fatalities',
        'dataset_id',
        'bicycle_fe_along',
        'location_id',
        'ped_fe',
        'car_fe',
        'ped_fe_crossing_side_road',
        'ped_fe_along_passenger_side',
        'car_fe_run_off_loc_passenger_side',
        'route_fatalities',
        'mc_fe_run_off_loc_driver_side',
        'fatalities_per_year',
        'ped_fe_along_driver_side',
        'bicycle_fe_run_off',
        'car_fe_property_access',
        'car_fe_head_on_loc',
        'mc_fe',
        'car_fe_head_on_overtaking',
        'ped_fe_crossing_through_road',
        'mc_fe_head_on_loc',
        'car_fe_run_off_loc_driver_side'
    ]
