# -*- coding: utf-8 -*-
"""
    ************************************************************************************************
    This file is for internal use by the ViDA SDK. It should not be altered by users
    ************************************************************************************************

    Controller for the Region resource. Overrides the BaseResourceController.


"""
from .resource import BaseResourceController


class RegionsController(BaseResourceController):

    def get_resource_path(self):
        return 'regions'
