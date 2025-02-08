# -*- coding: utf-8 -*-
"""
    ************************************************************************************************
    This file is for internal use by the ViDA SDK. It should not be altered by users
    ************************************************************************************************

    Controller for the DRIVER resource. Overrides the BaseResourceController.


"""
from .resource import BaseResourceController
from ..models.api_request import APIRequest


class DriverController(BaseResourceController):

    def get_resource_path(self):
        return 'driver'

    def get_map_star_ratings_for_dataset(self, dataset_id, filter_=None):
        """
            Gets before/after, smoothed/raw star ratings for a dataset, along with geo information
        :param dataset_id: 
        :param filter_: 
        :return: 
        """
        request = APIRequest(self.auth)
        request.set_url(self.get_resource_path(), 'map', ['dataset', dataset_id], filter_)
        request.get()
        return self.response(request)

    def get_modal_info_for_dataset(self, dataset_id, latitude, longitude, language, filter_=None):
        """
            Gets before star ratings, fatality estimations and suggested countermeasures
            for the road segment closet to latitude,longitude
        :param dataset_id:
        :param latitude:
        :param longitude:
        :param language:
        :param filter_:
        :return:
        """
        request = APIRequest(self.auth)
        request.set_url(self.get_resource_path(), 'info',
                        ['dataset', dataset_id, 'latitude', latitude, 'longitude', longitude, 'language', language], filter_)
        request.get()
        return self.response(request)

    def get_all_datasets(self, filter_=None):
        """
            Gets all the datasets for a user, including 'Final Published' ones
        :param filter_:
        :return:
        """
        request = APIRequest(self.auth)
        request.set_url(self.get_resource_path(), 'datasets', filter_)
        request.get()
        return self.response(request)
