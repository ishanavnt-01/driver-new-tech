# -*- coding: utf-8 -*-
"""
    ************************************************************************************************
    This file is for internal use by the ViDA SDK. It should not be altered by users
    ************************************************************************************************

    Controller for the Permission resource. Overrides the BaseResourceController.


"""
from .resource import BaseResourceController


class PermissionsController(BaseResourceController):

    def get_resource_path(self):
        return 'permissions'
