# -*- coding: utf-8 -*-
"""
     *************************************************************************************************
      This file is for internal use by the ViDA SDK. It should not be altered by users
    *************************************************************************************************
      
      Controller for the Star Ratings Restuls Summary resource. Overrides the Abstract Resource 
      Controller.
"""
from .resource import BaseResourceController


class StarRatingResultsSummaryController(BaseResourceController):

    def get_resource_path(self):
        return 'star-rating-results-summary'
