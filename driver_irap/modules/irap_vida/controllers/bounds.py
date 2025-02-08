# -*- coding: utf-8 -*-
"""
    ************************************************************************************************
    This file is for internal use by the ViDA SDK. It should not be altered by users
    ************************************************************************************************

    Controller for the Bounds resource. Overrides the BaseResourceController.


"""
from .resource import BaseResourceController


class BoundsController(BaseResourceController):

    def get_resource_path(self):
        return 'bounds'
