# -*- coding: utf-8 -*-
"""
    This is exactly like the response object, except that it has an additional accessor
    that returns any validation error objects that may have been returned (only if validation step
    of importation failed).
    
"""
from json import loads
from .response import Response


class ImportResponse(Response):

    def __init__(self, response):
        super().__init__(response.code, response.status, response.raw_response, response.error)

    @property
    def validation_errors(self):
        return loads(self.raw_response.decode('utf-8')) if self.raw_response else dict()

