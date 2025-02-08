# -*- coding: utf-8 -*-
"""
    ************************************************************************************************
    This file is for internal use by the ViDA SDK. It should not be altered by users
    ************************************************************************************************
    
    Contains the authentication methods used to request and set tokens. Also handles encryption
    of the user password, before it is transmitted to the API


"""
from urllib.parse import urlencode

from .resource import BaseResourceController
from ..models.api_request import APIRequest
from ..defines import IRAP_PERMISSIONS_LIVE_URL, IRAP_PERMISSIONS_URL


class Auth(BaseResourceController):

    def get_resource_path(self):
        return 'auth'

    @staticmethod
    def get_user_token(auth, email, password):
        """
            Sends the user's email and password to the API and gets a user token back. The user token 
            should be stored locally and used for all future requests. Email and password should NOT
            be stored locally. The password is encrypted using the APP_PRIVATE KEY before transmission.
        :param auth: 
        :param email: 
        :param password: 
        :return: 
        """
        encrypted_password = auth.get_encryption(password)
        request = APIRequest(auth)
        request.set_url('auth/register')
        request.data = dict(
            email=email,
            password=encrypted_password
        )
        request.post()

        return BaseResourceController.response(request)

    @staticmethod
    def request_user_permissions(auth, return_url):
        """
            Builds query parameters to sign, signs them and then sends the query to ViDA, so that the 
            user can view and accept/reject the permissions that the app is asking for.
        :param auth: BaseAuthentication object
        :param return_url: 
        """
        headers = auth.get_auth_headers()
        headers['return_url'] = return_url
        signatures = auth.get_signatures(headers)
        query = dict(headers, **signatures)
        url = IRAP_PERMISSIONS_URL if IRAP_PERMISSIONS_URL else IRAP_PERMISSIONS_LIVE_URL

        return '{}?{}'.format(url, urlencode(query))
