# -*- coding: utf-8 -*-
"""
    *************************************************************************************************
     This file is for internal use by the ViDA SDK. It should not be altered by users
    *************************************************************************************************
     
     Contains the primary methods for interacting with the API and are available to all resource
     controllers. These methods can be overridden in the individual resoruce controllers.
"""
from ..models.response import Response
from ..models.api_request import APIRequest


class BaseResourceController(object):

    def __init__(self, auth, filter_=None):
        self.auth = auth
        self.filter = filter_

    def get_resource_path(self):
        raise NotImplementedError

    def get_resource(self, id_=None, arguments=None):
        """
            
        :param id_: 
        :param arguments: 
        :return: Response object
        """
        request = APIRequest(self.auth)
        request.set_url(self.get_resource_path(), id_, arguments, self.filter)
        request.get()
        return self.response(request)

    def post_resource(self, data, id_=None, arguments=None):
        """
            * Send a POST request to the API. The resource and id make up the first two parts of the 
            * URL, are arguments can either be a third element, or an array of elements, each of which will
            * be separated with a '/'. data should be an array of name-value pairs, representing the 
            * names and values of the POST fields.
        :param data: 
        :param id_: 
        :param arguments: 
        :return: Response object
        """
        request = APIRequest(self.auth)
        request.set_url(self.get_resource_path(), id_, arguments)
        request.data = data
        request.post()
        return self.response(request)

    def put_request(self, id_, data, arguments=None):
        """
            Send a PUT request to the API. The resource and id make up the first two parts of the 
            URL, are arguments can either be a third element, or an array of elements, each of which will
            be separated with a '/'. data should be an array of name-value pairs, representing the 
            names and values of the POST fields.
        :param id_: 
        :param data: 
        :param arguments: 
        :return: Response object
        """
        request = APIRequest(self.auth)
        request.set_url(self.get_resource_path(), id_, arguments)
        request.data = data
        request.put()
        return self.response(request)

    def patch_resource(self, id_, data, arguments=None):
        """
            Send a PATCH request to the API. The resource and id make up the first two parts of the 
            URL, are arguments can either be a third element, or an array of elements, each of which will
            be separated with a '/'. data should be an array of name-value pairs, representing the 
            names and values of the POST fields.
        :param id_: 
        :param data: 
        :param arguments: 
        :return: Response object
        """
        request = APIRequest(self.auth)
        request.set_url(self.get_resource_path(), id_, arguments)
        request.data = data
        request.patch()
        return self.response(request)

    def delete_resource(self, id_, arguments=None):
        """
            Send a DELETE request to the API. The resource and id make up the first two parts of the 
            URL, are arguments can either be a third element, or an array of elements, each of which will
            be separated with a '/'.
        :param id_: 
        :param arguments: 
        :return: Response object
        """
        request = APIRequest(self.auth)
        request.set_url(self.get_resource_path(), id_, arguments)
        request.delete()
        return self.response(request)

    @staticmethod
    def response(request):
        """
            Takes the response properties from the APIRequest() object and formats them for use by
            the developer.
        :param request: request obj
        :return: Response object
        """
        return Response(
            code=request.code,
            status=request.status,
            raw_response=request.response,
            error=request.error
        )

