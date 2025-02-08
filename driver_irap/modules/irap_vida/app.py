# -*- coding: utf-8 -*-
"""
    Welcome to the ViDA SDK. This is the primary class for users of the SDK and contains all of the
    methods intended for use by developers. For help in understanding how to use the SDK, first 
    look at the README.md file, then read the comments on each of the methods listed below.
    
    If you require further help, or wish to report a bug fix, please email support@irap.org


"""
from .controllers.api import Api
from .models.app_authentication import AppAuthentication


class App(Api):

    def __init__(self, app_auth_id, app_api_key, app_private_key):
        """
            Start here! The constructor takes the App's authentication credentials, which will be 
            supplied to you by iRAP. An Authentication object
            is created, ready to be passed to the API as required           
        :param app_auth_id: integer 
        :param app_api_key: string
        :param app_private_key: string 
        """
        super(App, self).__init__()
        self.auth = AppAuthentication(
            app_auth_id=app_auth_id,
            app_api_key=app_api_key,
            app_private_key=app_private_key
        )

    def get_auth(self):
        return self.auth
