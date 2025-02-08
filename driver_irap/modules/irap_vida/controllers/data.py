# -*- coding: utf-8 -*-
"""
    ************************************************************************************************
    This file is for internal use by the ViDA SDK. It should not be altered by users
    ************************************************************************************************

    Controller for the Data resource. Overrides the BaseResourceController.


"""
from .resource import BaseResourceController
from ..models.api_request import APIRequest
from ..models.import_response import ImportResponse


class DataController(BaseResourceController):

    def get_resource_path(self):
        return 'data'

    def import_data(self, dataset_id, url):
        """
            Imports a CSV file from the specified url. The CSV file is expected to have a header
            row that will be ignored.

        :param dataset_id: the ID of the dataset we wish to import for.
        :param url: the url to the CSV file we wish to import. Temporary pre-signed s3 urls recommended.
        :return ImportResponse
        """
        request = APIRequest(self.auth)
        request.set_url(self.get_resource_path(), dataset_id, ['import'])
        request.data = dict(url=url)
        request.post()
        return ImportResponse(self.response(request))

