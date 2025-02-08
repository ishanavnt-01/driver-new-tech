# -*- coding: utf-8 -*-
"""
    A response object to represent the response that came back from the API.
    This object has been limited by a need to not break an existing interface, but when possible
    it will be good to make the following changes
        - change member variables to be private
        - only one response member variable, which is the raw response from the API.
        - getResponse() should become getJsonResponse() which performs last minute json_decode.
        - status should possibly become a boolean because values 

"""
from json import loads


class Response(object):

    def __init__(self, code, status, raw_response, error=None):

        """
            Create the response object from an API response.
        :param code: the HTTP response code.
        :param status: the status message from the response header.
        :param raw_response: the string response body.
        :param error: the error message from the API response header (if there was one)
        """
        if status is None:
            # if status is not set, something went wrong.
            status = 'Error'

            if error is None:
                error = 'Server did not respond as expected (server may be down)'

        if status not in ['Success', 'Error']:
            raise Exception('Unrecognized status: {}'.format(status))

        self.status = status
        self.raw_response = raw_response
        self.code = code
        self.error = error
        self.response = loads(raw_response.decode('utf-8')) if raw_response and code == 200 else None


