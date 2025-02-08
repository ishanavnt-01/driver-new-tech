# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .base_model import BaseModel


class Country(BaseModel):

    attributes = [
        'name',
        'country_code_2_digit',
        'land_area',
        'country_code_3_digit',
        'gdp_per_capita',
        'id',
        'currency_code',
        'population'
    ]
