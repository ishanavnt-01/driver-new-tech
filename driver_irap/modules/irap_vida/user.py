# -*- coding: utf-8 -*-
"""
    Welcome to the ViDA SDK. This is the primary class for accessing the API as a User and contains 
    all of the methods intended for use by developers. For help in understanding how to use the SDK, 
    first look at the README.md file, then read the comments on each of the methods listed below.
    
    If you require further help, or wish to report a bug fix, please email support@irap.org

"""
from .controllers.api import Api
from .models.user_authentication import UserAuthentication


class User(Api):

    def __init__(self, app_auth_id, app_api_key, app_private_key, user_auth_id, user_api_key, user_private_key):
        """
            Start here! The constructor takes the App's authentication credentials, which will be 
            supplied to you by iRAP and the user's authentication details. An Authentication object
            is created, ready to be passed to the API as required.
            
        :param app_auth_id: 
        :param app_api_key: 
        :param app_private_key: 
        :param user_auth_id: 
        :param user_api_key: 
        :param user_private_key: 
        """
        super(User, self).__init__()
        self.auth = UserAuthentication(
            app_auth_id=app_auth_id,
            app_api_key=app_api_key,
            app_private_key=app_private_key,
            user_auth_id=user_auth_id,
            user_api_key=user_api_key,
            user_private_key=user_private_key
        )

    def get_auth(self):
        return self.auth
