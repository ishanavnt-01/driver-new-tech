# -*- coding: utf-8 -*-
"""
    *************************************************************************************************
    This file is for internal use by the ViDA SDK. It should not be altered by users
    *************************************************************************************************
    
    This class deals with the authentication. It generates the signatures for app and user and makes
    them available to the APIRequest object, for sending with all API requests. 
"""
from __future__ import absolute_import

from .base_authentication import BaseAuthentication


class AppAuthentication(BaseAuthentication):

    def __init__(self, app_auth_id, app_api_key, app_private_key):
        """
            Takes the API token and sets up the authentication member variable
            
        :param app_auth_id: int
        :param app_api_key: string
        :param app_private_key: string
        """
        super(AppAuthentication, self).__init__()
        self.app_auth_id = app_auth_id
        self.app_api_key = app_api_key
        self.app_private_key = app_private_key
        self.auth_headers = dict(
            auth_system_auth_id=app_auth_id,
            auth_system_public_key=app_api_key
        )

    def get_encryption(self, msg):
        """
            Encrypts a string
            
        :param msg: 
        :return: 
        """
        return self.encrypt(msg, self.app_private_key)

    def get_auth_headers(self):
        """
            Builds an array of request parameters, for sending the API
        :return: 
        """
        return dict(
            auth_system_auth_id=self.app_auth_id,
            auth_system_public_key=self.app_api_key
        )

    def get_signatures(self, data):
        """
            Gets the signature for app and returns as an array.
        
        :param data: 
        :return: 
        """
        return dict(auth_system_signature=self.generate_signature(data, self.app_private_key))
