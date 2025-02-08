# -*- coding: utf-8 -*-
"""
    *************************************************************************************************
     This file is for internal use by the ViDA SDK. It should not be altered by users
    *************************************************************************************************
     
     This is the class that actually makes the requests to the API. It is essentially a wrapper
     for CURL, which handles inclusion of the authentication tokens in the header, assembles the
     URL, sets the different request types and handles the response from the API.
     
    
"""
from __future__ import absolute_import

import requests
import random
from base64 import b64encode
from time import time
from json import dumps

from ..defines import IRAP_API_LIVE_URL, IRAP_API_URL, IRAP_API_VERSION


class APIRequest(object):

    def __init__(self, auth):
        """
            The constructor feeds the authentication information into the request headers.
        :param auth: AppAuthentication model
        """
        self.auth = auth
        self.base_url = IRAP_API_URL if IRAP_API_URL else IRAP_API_LIVE_URL  # the url of where the API is. e.g. https://api.vida.irap.org (no end slash)
        self.headers = dict()
        self.data = dict()  # array of data to send off in the body of the request
        self.url_no_filter = None  # url that will never have any ?filter within it
        self.url = None  # url request will be sent to
        self.ch = None
        self.code = None
        self.status = None
        self.response = None
        self.error = None

    def set_url(self, resource, id_=None, arguments=None, filter_=None):

        url = self.base_url

        if IRAP_API_VERSION:
            url = '{}/{}'.format(url, IRAP_API_VERSION)

        url = '{}/{}'.format(url, resource)

        if id_:
            url = '{}/{}'.format(url, id_)

        if arguments:
            if isinstance(arguments, dict):
                new_args = []
                for k, v in arguments.items():
                    new_args.append(k)
                    new_args.append(str(v))
                new_args = '/'.join(new_args)

            elif isinstance(arguments, list):
                new_args = '/'.join([str(i) for i in arguments])

            else:
                new_args = str(arguments)

            url = '{}/{}'.format(url, new_args)

        self.url_no_filter = url

        if filter_:
            self.headers.update(dict(
                filter=filter_.get_filter()
            ))

        self.url = url

    def _prepare_headers(self):

        v = bytearray(random.getrandbits(8) for _ in range(20))
        nonce = b64encode(v)[:20]

        # Headers that need to be renewed every time we hit send()
        last_second_headers = dict(
            auth_nonce=nonce.decode('utf-8'),
            auth_timestamp=int(time())
        )

        headers = dict()
        headers.update(self.headers)
        headers.update(self.auth.get_auth_headers())
        headers.update(last_second_headers)

        all_data_to_sign = dict()
        all_data_to_sign.update(headers)
        all_data_to_sign.update(self.data)
        all_data_to_sign['auth_url'] = self.url_no_filter

        signatures = self.auth.get_signatures(all_data_to_sign)
        headers.update(signatures)
        self.headers = headers

    def get(self):
        self._prepare_headers()
        response = requests.get(self.url, headers={k: str(v) if not isinstance(v, str) else v for k, v in self.headers.items()})
        self.process_response(response)

    def post(self):
        self._prepare_headers()
        response = requests.post(
            url=self.url,
            headers={k: str(v) if not isinstance(v, str) else v for k, v in self.headers.items()},
            data=dumps(self.data, separators=(',', ':'), sort_keys=True)
        )
        self.process_response(response)

    def put(self):
        self._prepare_headers()
        response = requests.put(
            url=self.url,
            headers={k: str(v) if not isinstance(v, str) else v for k, v in self.headers.items()},
            data=dumps(self.data, separators=(',', ':'), sort_keys=True)
        )
        self.process_response(response)

    def patch(self):
        self._prepare_headers()
        response = requests.patch(
            url=self.url,
            headers={k: str(v) if not isinstance(v, str) else v for k, v in self.headers.items()},
            data=dumps(self.data, separators=(',', ':'), sort_keys=True)
        )
        self.process_response(response)

    def delete(self):
        self._prepare_headers()
        response = requests.delete(
            url=self.url,
            headers={k: str(v) if not isinstance(v, str) else v for k, v in self.headers.items()}
        )
        self.process_response(response)

    def process_response(self, response):
        self.code = response.status_code
        self.response = response.content

        self.status = response.headers.get('API_STATUS', None)
        self.error = response.headers.get('Error', None) if response.status_code != 200 else None


