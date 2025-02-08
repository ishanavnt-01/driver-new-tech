# -*- coding: utf-8 -*-
"""
    ************************************************************************************************
    This file is for internal use by the ViDA SDK. It should not be altered by users
    ************************************************************************************************

    Controller for the Star Rating resource. Overrides the BaseResourceController.


"""
from .resource import BaseResourceController
from ..models.api_request import APIRequest


class StarRatingsController(BaseResourceController):

    def get_resource_path(self):
        return 'star-ratings'

    def get_before_star_ratings(self, id_, dataset_id, filter_=None):
        """
            Fetches a before countermeasures star rating for a specified location. You must specify the 
            location ID, and the ID of the dataset it belongs to.
        :param id_: the ID of the location
        :param dataset_id: the ID of the dataset the location relates to
        :param filter_: 
        :return: 
        """
        request = APIRequest(self.auth)
        request.set_url(self.get_resource_path(), id_, dict(before=dataset_id), filter_)
        request.get()
        return self.response(request)

    def get_before_star_ratings_for_dataset(self, dataset_id, filter_=None):
        """
            Get a list of star ratings for a dataset, using the dataset's ID.
        :param dataset_id: 
        :param filter_: 
        :return: 
        """
        request = APIRequest(self.auth)
        request.set_url(self.get_resource_path(), 'for', ['dataset', dataset_id, 'before'], filter_)
        request.get()
        return self.response(request)

    def get_after_star_ratings_for_dataset(self, dataset_id, filter_=None):
        """
            Get a list of star ratings for a dataset, using the dataset's ID.
        :param dataset_id: 
        :param filter_: 
        :return: 
        """
        request = APIRequest(self.auth)
        request.set_url(self.get_resource_path(), 'for', ['dataset', dataset_id, 'after'], filter_)
        request.get()
        return self.response(request)
