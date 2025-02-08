# -*- coding: utf-8 -*-
"""
    ************************************************************************************************
    This file is for internal use by the ViDA SDK. It should not be altered by users
    ************************************************************************************************

    Controller for the Dataset resource. Overrides the BaseResourceController.


"""
from .resource import BaseResourceController


class DatasetsController(BaseResourceController):

    def get_resource_path(self):
        return 'datasets'
