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


class UserAuthentication(BaseAuthentication):

    def __init__(self, app_auth_id, app_api_key, app_private_key, user_auth_id, user_api_key, user_private_key):
        """
            Takes the API token and user token if available and sets up the authentication member variable
        :param app_auth_id: 
        :param app_api_key: 
        :param app_private_key: 
        :param user_auth_id: 
        :param user_api_key: 
        :param user_private_key: 
        """
        super(UserAuthentication, self).__init__()
        self.app_auth_id = app_auth_id
        self.app_api_key = app_api_key
        self.app_private_key = app_private_key
        self.user_auth_id = user_auth_id
        self.user_api_key = user_api_key
        self.user_private_key = user_private_key

    def get_auth_headers(self):
        """
            Builds an dict of request parameters, for sending the API
        :return: dictionary
        """
        return dict(
            auth_system_auth_id=self.app_auth_id,
            auth_system_public_key=self.app_api_key,
            auth_user_auth_id=self.user_auth_id,
            auth_user_public_key=self.user_api_key
        )

    def get_signatures(self, data):
        """
            Gets the user and app signatures for the provided data 
            before returning them as an assosciative array.
        :param data: 
        :return: dictionary
        """
        return dict(
            auth_system_signature=self.generate_signature(data, self.app_private_key),
            auth_user_signature=self.generate_signature(data, self.user_private_key)
        )
