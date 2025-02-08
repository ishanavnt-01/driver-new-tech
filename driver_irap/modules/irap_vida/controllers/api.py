# -*- coding: utf-8 -*-
"""
    This interface specifies the methods available to users of the ViDA SDK. It is implemented by
    App and User

"""
from ..controllers.auth import Auth
from ..controllers.projects import ProjectsController
from ..controllers.users import UsersController
from ..controllers.datasets import DatasetsController
from ..controllers.programmes import ProgrammesController
from ..controllers.regions import RegionsController
from ..controllers.variables import VariablesController
from ..controllers.road_attributes import RoadAttributesController
from ..controllers.locations import LocationsController
from ..controllers.bounds import BoundsController
from ..controllers.fatalities import FatalitiesController
from ..controllers.star_ratings import StarRatingsController
from ..controllers.data import DataController
from ..controllers.countries import CountriesController
from ..controllers.permissions import PermissionsController
from ..controllers.star_rating_results_summary import StarRatingResultsSummaryController
from ..controllers.report_filters import ReportFiltersController
from ..controllers.driver import DriverController


class Api(object):

    def __init__(self):
        super(Api, self).__init__()
        self.auth = None

    def get_auth(self):
        raise NotImplementedError

    def get_user_token(self, email, password):
        """
            ******* This method requires special permission from iRAP and is not available to all ********
    
            Takes the user's email address and password and returns the user authentication token needed
            to complete all future requests made on behalf of that user. The email address and password
            should not be stored in your app as they are no longer needed. The returned token can be used
            by calling the setUserToken() method below, and should be stored for future use, to avoid 
            having to ask the user to sign in again.
        :param email: 
        :param password: 
        :return: 
        """
        user_token = Auth.get_user_token(self.auth, email, password)
        token = dict()
        if user_token.code == 200:
            token['user_auth_id'] = user_token.response.get('auth_id', None)
            token['user_api_key'] = user_token.response.get('api_key', None)
            token['user_private_key'] = user_token.response.get('api_secret', None)
            token['user_id'] = user_token.response.get('user_id', None)

        token['status'] = user_token.status
        token['code'] = user_token.code
        token['error'] = user_token.error

        return token

    def request_user_permissions(self, return_url):
        """
            Checks whether the GET contains the user key. If so, it creates a user token object and 
            returns it. If not, the requestUserPermissions method is called, which redirects the user
            to ViDA.
        :param return_url: 
        """
        return Auth.request_user_permissions(self.auth, return_url)

    def get_users(self, id_=None, filter_=None):
        """
            Fetches a list of all of the users in the system. If you specify an ID, that user will be
            returned to you.
        :param id_: 
        :param filter_: 
        :return: 
        """

        return UsersController(self.auth, filter_).get_resource(id_)

    def add_user(self, name, email, password):

        """
            Add a new user to the system by supplying their name, email address and a password.
        :param name: 
        :param email: 
        :param password: 
        """
        return UsersController(self.auth).post_resource(data=dict(name=name, email=email, password=password))

    def update_user(self, id_, name=None, email=None, password=None):

        """
            Update a user in the system by supplying their user id, along with a new name, email address
            and password.
        :param id_: 
        :param name: 
        :param email: 
        :param password: 
        """
        return UsersController(self.auth).patch_resource(id_, data=dict(name=name, email=email, password=password))

    def replace_user(self, id_, name=None, email=None, password=None, delimiter=None, decimal_mark=None):
        """
            Replace a user in the system by supplying their user id, along with a new name, email address
            and password.
        :param id_: 
        :param name: 
        :param email: 
        :param password: 
        :param delimiter: 
        :param decimal_mark: 
        """
        return UsersController(self.auth).put_request(
            id_,
            data=dict(name=name, email=email, password=password, delimiter=delimiter, decimal_mark=decimal_mark)
        )

    def delete_user(self, id_):

        """
            Delete a user from the system, using their user id.
        :param id_: 
        """
        return UsersController(self.auth).delete_resource(id_)

    def get_user_access(self, id_=None, filter_=None):
        """
            Fetches a list of all of the users in the system. If you specify an ID, that user will be
            be returned to you.
        :param id_:
        :param filter_:
        :return:
        """

        return UsersController(self.auth, filter_).get_resource(id_, 'user-access')

    def get_datasets(self, id_=None, filter_=None):

        """
            Fetches a list of all of the datasets in the system. If you specify an ID, that dataset will
            be returned to you.
        :param id_: 
        :param filter_: 
        :return: 
        """

        return DatasetsController(self.auth, filter_).get_resource(id_)

    def add_dataset(self, name, project_id, manager_id):

        """
            Creates a new dataset using the supplied data, which should be an array of field name as 
            keys and the values you wish to insert, as name-value pairs
        :param name: 
        :param project_id: 
        :param manager_id: 
        """

        return DatasetsController(self.auth).post_resource(
            data=dict(name=name, project_id=project_id, manager_id=manager_id)
        )

    def update_dataset(self, id_, name=None, project_id=None, manager_id=None):

        """
            Updates a dataset using the supplied data, which should be an array of field name as 
            keys and the values you wish to insert, as name-value pairs. The ID of the dataset to update
            and a new name should also be supplied
        :param id_: 
        :param name: 
        :param project_id: 
        :param manager_id: 
        """
        return DatasetsController(self.auth).patch_resource(
            id_=id_,
            data=dict(name=name, project_id=project_id, manager_id=manager_id)
        )

    def update_dataset_status(self, id_, status_id):

        """
            Updates the status of a dataset, using the following status codes:
            
                1 - Draft
                2 - Working
                3 - Final Hidden
                4 - Final Unpublished
                5 - Final Published
                
        :param id_: 
        :param status_id: 
        """
        return DatasetsController(self.auth).patch_resource(
            id_=id_,
            data=dict(status_id=status_id)
        )

    def replace_dataset(self, id_, name=None, project_id=None, manager_id=None, status_id=None):
        # TODO: Error "There is no projects object with id: 0"
        """
            Replaces a dataset using the supplied data, which should be an array of field name as 
            keys and the values you wish to insert, as name-value pairs. The ID of the dataset to replace
            and a new name should also be supplied
        :param id_: 
        :param name: 
        :param project_id: 
        :param manager_id: 
        """
        return DatasetsController(self.auth).put_request(
            id_=id_,
            data=dict(name=name, project_id=project_id, manager_id=manager_id, status_id=status_id)
        )

    def delete_dataset(self, id_):

        """
            Deletes a dataset from the system, using the dataset's ID.
        :param id_: 
        """
        return DatasetsController(self.auth).delete_resource(id_)

    def get_dataset_users(self, id_, filter_=None):

        """
            Get a list of the users who have access to a dataset, using the dataset's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return DatasetsController(self.auth, filter_).get_resource(id_, 'user-access')

    def add_dataset_user(self, dataset_id, user_id, access_level=1, user_manager=0):

        """
            Grant access to the specified user for the specified dataset
        :param dataset_id: 
        :param user_id: 
        :param access_level: 
        :param user_manager: 
        """
        return DatasetsController(self.auth).post_resource(
            data=dict(user_id=user_id, access_level=access_level, user_manager=user_manager),
            id_=dataset_id,
            arguments='user-access'
        )

    def delete_dataset_user(self, dataset_id, user_id):

        """
            Revokes access for the specified user for the specified dataset
        :param dataset_id: 
        :param user_id: 
        """

        return DatasetsController(self.auth).delete_resource(
            id_=dataset_id,
            arguments={'user-access': user_id}
        )

    def get_datasets_for_programme(self, id_, filter_=None):

        """
            Get a list of datasets for a programme, using the programme's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return DatasetsController(self.auth, filter_).get_resource('for', dict(programme=id_))

    def get_datasets_for_region(self, id_, filter_=None):

        """
            Get a list of datasets for a region, using the region's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return DatasetsController(self.auth, filter_).get_resource('for', dict(region=id_))

    def get_datasets_for_project(self, id_, filter_=None):

        """
            Get a list of datasets for a project, using the project's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return DatasetsController(self.auth, filter_).get_resource('for', dict(project=id_))

    def process_dataset(self, id_, filter_=None):

        """
            Start processing the specified dataset. Processing data is added to a queue and a successful
            response to this request means that the dataset has been added to the queue, not that
            processing is complete. To check whether it has finished, call get_dataset(id) and examine
            the returned is_data_processing property.
        :param id_: 
        :param filter_: 
        :return: 
        """

        return DatasetsController(self.auth, filter_).get_resource(id_, 'process')

    def validate_and_process_dataset(self, id_, filter_=None):

        """
            Validates the specified dataset and begins processing. Processing data is added to a queue 
            and a successful response to this request means that the dataset has been added to the queue, 
            not that processing is complete. To check whether it has finished, call get_dataset($id) and 
            examine the returned is_data_processing property.
            
            If the data fails to validate, a 400 will be returned. Check the errors property for the 
            errors encountered
        :param id_: 
        :param filter_: 
        :return: 
        """
        return DatasetsController(self.auth, filter_).get_resource(id_, 'validateandprocess')

    def get_programmes(self, id_=None, filter_=None):

        """
            Fetches a list of all of the programmes in the system. If you specify an ID, that programme
            will be returned to you.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return ProgrammesController(self.auth, filter_).get_resource(id_)

    def add_programme(self, name, manager_id):

        """
            Creates a new programme, for which a name should be supplied, along with the user id of 
            the programme's manager.
        :param name: 
        :param manager_id: 
        """
        return ProgrammesController(self.auth).post_resource(data=dict(name=name, manager_id=manager_id))

    def update_programme(self, id_, name=None, manager_id=None):

        """
            Updates a programme, for which a new name should be supplied, along with the id of
            the programme, and the user id of the programme's manager.
        :param id_: 
        :param name: 
        :param manager_id: 
        """
        return ProgrammesController(self.auth).patch_resource(id_, data=dict(name=name, manager_id=manager_id))

    def replace_programme(self, id_, name=None, manager_id=None):
        # TODO: Error "principal_users_id has not yet been created in the mysql table for: iRAP\\VidaDb\\Programme"

        """
            Replaces a programme, for which a new name should be supplied, along with the id of
            the programme, and the user id of the programme's manager.
        :param id_: 
        :param name: 
        :param manager_id: 
        """
        return ProgrammesController(self.auth).put_request(id_, data=dict(name=name, manager_id=manager_id))

    def delete_programme(self, id_):

        """
            Deletes a programme from the system, using the programme's ID.
        :param id_: 
        """
        return ProgrammesController(self.auth).delete_resource(id_)

    def get_programme_users(self, id_, filter_=None):

        """
            Get a list of the users who have access to a programme, using the programme's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return ProgrammesController(self.auth, filter_).get_resource(id_, 'user-access')

    def add_programme_user(self, programme_id, user_id, access_level=1, user_manager=0):

        """
            Grant access to the specified user for the specified programme
        :param programme_id: 
        :param user_id: 
        :param access_level: 
        :param user_manager: 
        """
        return ProgrammesController(self.auth).post_resource(
            data=dict(user_id=user_id, access_level=access_level, user_manager=user_manager),
            id_=programme_id,
            arguments='user-access'
        )

    def delete_programme_user(self, programme_id, user_id):

        """
            Revokes access for the specified user for the specified programme
        :param programme_id: 
        :param user_id: 
        """
        return ProgrammesController(self.auth).delete_resource(
            id_=programme_id,
            arguments={'user-access': user_id}
        )

    def get_regions(self, id_=None, filter_=None):

        """
            Fetches a list of all of the regions in the system. If you specify an ID, that region will be
            returned to you.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return RegionsController(self.auth, filter_).get_resource(id_)

    def add_region(self, name, programme_id, manager_id):

        """
            Creates a new region, for which a name should be supplied, along with the id of the parent
            programme and the user id of the region's manager.
        :param name: 
        :param programme_id: 
        :param manager_id: 
        """
        return RegionsController(self.auth).post_resource(
            data=dict(name=name, programme_id=programme_id, manager_id=manager_id)
        )

    def update_region(self, id_, name=None, programme_id=None, manager_id=None):

        """
            Updates a region, for which a new name should be supplied, along with the id of the 
            region, the id of the parent programme and the user id of the region's manager.
        :param id_: 
        :param name: 
        :param programme_id: 
        :param manager_id: 
        """
        return RegionsController(self.auth).patch_resource(id_,
            data=dict(name=name, programme_id=programme_id, manager_id=manager_id)
        )

    def replace_region(self, id_, name=None, programme_id=None, manager_id=None):
        # TODO: Error "parent_id has not yet been created in the mysql table for: iRAP\\VidaDb\\Region"
        """
            Replaces a region, for which a new name should be supplied, along with the id of the 
            region, the id of the parent programme and the user id of the region's manager.
        :param id_: 
        :param name: 
        :param programme_id: 
        :param manager_id: 
        """
        return RegionsController(self.auth).put_request(id_,
            data=dict(name=name, programme_id=programme_id, manager_id=manager_id)
        )

    def delete_region(self, id_):

        """
            Deletes a region from the system, using the region's ID.
        :param id_: 
        """
        return RegionsController(self.auth).delete_resource(id_)

    def get_region_users(self, id_, filter_=None):

        """
            Get a list of the users who have access to a region, using the region's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return RegionsController(self.auth, filter_).get_resource(id_, 'user-access')

    def add_region_user(self, region_id, user_id, access_level=1, user_manager=0):

        """
            Grant access to the specified user for the specified region
        :param region_id: 
        :param user_id: 
        :param access_level: 
        :param user_manager: 
        """
        return RegionsController(self.auth).post_resource(
            data=dict(user_id=user_id, access_level=access_level, user_manager=user_manager),
            id_=region_id,
            arguments='user-access'
        )

    def delete_region_user(self, region_id, user_id):

        """
            Revokes access for the specified user for the specified region
        :param region_id: 
        :param user_id: 
        """
        return RegionsController(self.auth).delete_resource(
            id_=region_id,
            arguments={'user-access': user_id}
        )

    def get_regions_for_programme(self, id_, filter_=None):

        """
            Get a list of regions for a programme, using the programme's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return RegionsController(self.auth, filter_).get_resource('for', dict(programme=id_))

    def get_projects(self, id_=None, filter_=None):
        """
            Fetches a list of all of the projects in the system. If you specify an ID, that project will
            be returned to you.
        :param id_: int
        :param filter_: 
        :return: Response object
        """
        return ProjectsController(self.auth, filter_).get_resource(id_)

    def add_project(self, name, region_id, manager_id, model_id, country_id):

        """
            Creates a new project, for which a name should be supplied, along with the id of the parent
            region, the user id of the project's manager and the id of the model to be used.
        :param name: 
        :param region_id: 
        :param manager_id: 
        :param model_id: 
        :param country_id: 
        """
        return ProjectsController(self.auth).post_resource(
            data=dict(name=name, region_id=region_id, manager_id=manager_id, model_id=model_id, country_id=country_id)
        )

    def update_project(self, id_, name=None, region_id=None, manager_id=None):

        """
            Updates a project, for which a name should be supplied, along with the id of the 
            project, the id of the parent region, the user id of the project's manager.
        :param id_: 
        :param name: 
        :param region_id: 
        :param manager_id: 
        """
        return ProjectsController(self.auth).patch_resource(
            id_=id_,
            data=dict(name=name, region_id=region_id, manager_id=manager_id)
        )

    def replace_project(self, id_, name=None, region_id=None, manager_id=None):
        # TODO: Error "raps_id has not yet been created in the mysql table for: iRAP\\VidaDb\\Project"
        """
            Replaces a project, for which a name should be supplied, along with the id of the 
            project, the id of the parent region, the user id of the project's manager.
        :param id_: 
        :param name: 
        :param region_id: 
        :param manager_id: 
        """
        return ProjectsController(self.auth).put_request(
            id_=id_,
            data=dict(name=name, region_id=region_id, manager_id=manager_id)
        )

    def delete_project(self, id_):

        """
            Deletes a project from the system, using the project's ID.
        :param id_: 
        """
        return ProjectsController(self.auth).delete_resource(id_)

    def add_project_user(self, project_id, user_id, access_level=1, user_manager=0):

        """
            Grant access to the specified user for the specified project
        :param project_id: 
        :param user_id: 
        :param access_level: 
        :param user_manager: 
        """
        return ProjectsController(self.auth).post_resource(
            data=dict(user_id=user_id, access_level=access_level, user_manager=user_manager),
            id_=project_id,
            arguments='user-access'
        )

    def delete_project_user(self, project_id, user_id):

        """
            Revokes access for the specified user for the specified project
        :param project_id: 
        :param user_id: 
        """
        return ProjectsController(self.auth).delete_resource(
            id_=project_id,
            arguments={'user-access': user_id}
        )

    def get_project_users(self, id_, filter_=None):

        """
            Get a list of the users who have access to a project, using the project's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return ProjectsController(self.auth, filter_).get_resource(id_, 'user-access')

    def get_projects_for_programme(self, id_, filter_=None):

        """
            Get a list of projects for a programme, using the programme's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return ProjectsController(self.auth, filter_).get_resource('for', dict(programme=id_))

    def get_projects_for_region(self, id_, filter_=None):

        """
            Get a list of projects for a regions, using the regions's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return ProjectsController(self.auth, filter_).get_resource('for', dict(region=id_))

    def get_variables(self, id_=None, filter_=None):

        """
            Fetches a list of all of the variables in the system. If you specify an ID, that variable will be
            returned to you.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return VariablesController(self.auth, filter_).get_resource(id_)

    def update_variable(self, id_, variables):

        """
            Updates a set of variables, using the values supplied. $variables should be an array list
            of name-value pairs, where the name matches the relevant field in the database. $id should
            be the ID of the set of variables to be updated
        :param id_: 
        :param variables: 
        """
        if not isinstance(variables, dict):
            raise ValueError('Wrong data type for variables')

        return VariablesController(self.auth).patch_resource(
            id_=id_,
            data=variables
        )

    def replace_variable(self, id_, variables):
        # TODO: Error 502
        """
            Replaces a set of variables, using the values supplied. $variables should be an array list
            of name-value pairs, where the name matches the relevant field in the database. $id should
            be the ID of the set of variables to be replaced
        :param id_: 
        :param variables: 
        """
        if not isinstance(variables, dict):
            raise ValueError('Wrong data type for variables')

        return VariablesController(self.auth).put_request(
            id_=id_,
            data=variables
        )

    def get_variables_for_dataset(self, id_, filter_=None):

        """
            Get a list of variables for a dataset, using the dataset's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return VariablesController(self.auth, filter_).get_resource('for', dict(dataset=id_))

    def get_before_road_attributes(self, id_, dataset_id, filter_=None):
        # TODO: Provjerit!
        """
            Fetches a road attributes for a specified location. You must specify the location ID,
            and the ID of the dataset it belongs to.
        :param id_: the location ID of the road attributes in the dataset.
        :param dataset_id: the ID of the dataset.
        :param filter_: 
        :return: 
        """
        return RoadAttributesController(self.auth, filter_).get_resource(id_, dict(before=dataset_id))

    def get_after_road_attributes(self, id_, dataset_id, filter_=None):
        # TODO: Provjerit!
        """
            Fetches a road attributes for a specified location. You must specify the location ID,
            and the ID of the dataset it belongs to.
        :param id_: the location ID of the road attributes in the dataset.
        :param dataset_id: the ID of the dataset.
        :param filter_: 
        :return: 
        """
        return RoadAttributesController(self.auth, filter_).get_resource(id_, dict(after=dataset_id))

    def get_before_road_attributes_for_programme(self, id_, filter_=None):

        """
            Get a list of road attributes for a programme, using the programme's ID.
        :param id_: the ID of the programme.
        :param filter_: 
        :return: 
        """
        return RoadAttributesController(self.auth, filter_).get_resource('for', ['programme', id_, 'before'])

    def get_after_road_attributes_for_programme(self, id_, filter_=None):

        """
            Get a list of road attributes for a programme, using the programme's ID.
        :param id_: the ID of the programme.
        :param filter_: 
        :return: 
        """
        return RoadAttributesController(self.auth, filter_).get_resource('for', ['programme', id_, 'after'])

    def get_before_road_attributes_for_region(self, id_, filter_=None):

        """
            Get a list of road attributes for a region, using the region's ID.
        :param id_: the ID of the region.
        :param filter_: 
        :return: 
        """
        return RoadAttributesController(self.auth, filter_).get_resource('for', ['region', id_, 'before'])

    def get_after_road_attributes_for_region(self, id_, filter_=None):

        """
            Get a list of road attributes for a region, using the region's ID.
        :param id_: the ID of the region.
        :param filter_: 
        :return: 
        """
        return RoadAttributesController(self.auth, filter_).get_resource('for', ['region', id_, 'after'])

    def get_before_road_attributes_for_project(self, id_, filter_=None):

        """
            Get a list of road attributes for a project.
        :param id_: the ID of the project to get road attributes for.
        :param filter_: 
        :return: 
        """
        return RoadAttributesController(self.auth, filter_).get_resource('for', ['project', id_, 'before'])

    def get_after_road_attributes_for_project(self, id_, filter_=None):

        """
            Get a list of road attributes for a project.
        :param id_: the ID of the project to get road attributes for.
        :param filter_: 
        :return: 
        """
        return RoadAttributesController(self.auth, filter_).get_resource('for', ['project', id_, 'after'])

    def get_before_road_attributes_for_dataset(self, id_, filter_=None):

        """
            Get a list of road attributes for a dataset, using the dataset's ID.
        :param id_: the ID of the dataset
        :param filter_: 
        :return: 
        """
        return RoadAttributesController(self.auth, filter_).get_resource('for', ['dataset', id_, 'before'])

    def get_after_road_attributes_for_dataset(self, id_, filter_=None):

        """
            Get a list of road attributes for a dataset, using the dataset's ID.
        :param id_: the ID of the dataset
        :param filter_: 
        :return: 
        """
        return RoadAttributesController(self.auth, filter_).get_resource('for', ['dataset', id_, 'after'])

    def get_before_locations(self, id_, dataset_id, filter_=None):
        # TODO: Provjerit
        """
            Fetches a locations for a specified location. You must specify the location ID,
            and the ID of the dataset it belongs to.
        :param id_: the location ID of the locations in the dataset.
        :param dataset_id: the ID of the dataset.
        :param filter_: 
        :return: 
        """
        return LocationsController(self.auth, filter_).get_resource(id_, dict(before=dataset_id))

    def get_before_locations_for_programme(self, id_, filter_=None):

        """
            Get a list of locations for a programme, using the programme's ID.
        :param id_: the ID of the programme.
        :param filter_: 
        :return: 
        """
        return LocationsController(self.auth, filter_).get_resource('for', ['programme', id_, 'before'])

    def get_before_locations_for_regions(self, id_, filter_=None):

        """
            Get a list of locations for a region, using the region's ID.
        :param id_: the ID of the region.
        :param filter_: 
        :return: 
        """
        return LocationsController(self.auth, filter_).get_resource('for', ['region', id_, 'before'])

    def get_before_locations_for_project(self, id_, filter_=None):

        """
            Get a list of locations for a project.
        :param id_: the ID of the project to get locations for.
        :param filter_: 
        :return: 
        """
        return LocationsController(self.auth, filter_).get_resource('for', ['project', id_, 'before'])

    def get_before_locations_for_dataset(self, id_, filter_=None):

        """
            Get a list of locations for a dataset, using the dataset's ID.
        :param id_: the ID of the dataset
        :param filter_: 
        :return: 
        """
        return LocationsController(self.auth, filter_).get_resource('for', ['dataset', id_, 'before'])

    def get_after_locations(self, id_, dataset_id, filter_=None):
        # TODO: Provjerit
        """
            Fetches a locations for a specified location. You must specify the location ID,
            and the ID of the dataset it belongs to.
        :param id_: the location ID of the locations in the dataset.
        :param dataset_id: the ID of the dataset.
        :param filter_: 
        :return: 
        """
        return LocationsController(self.auth, filter_).get_resource(id_, [dataset_id, 'after'])

    def get_after_locations_for_programme(self, id_, filter_=None):

        """
            Get a list of locations for a programme, using the programme's ID.
        :param id_: the ID of the programme.
        :param filter_: 
        :return: 
        """
        return LocationsController(self.auth, filter_).get_resource('for', ['programme', id_, 'after'])

    def get_after_locations_for_region(self, id_, filter_=None):

        """
            Get a list of locations for a region, using the region's ID.
        :param id_: the ID of the region.
        :param filter_: 
        :return: 
        """
        return LocationsController(self.auth, filter_).get_resource('for', ['region', id_, 'after'])

    def get_after_locations_for_project(self, id_, filter_=None):

        """
            Get a list of locations for a project.
        :param id_: the ID of the project to get locations for.
        :param filter_: 
        :return: 
        """
        return LocationsController(self.auth, filter_).get_resource('for', ['project', id_, 'after'])

    def get_after_locations_for_dataset(self, id_, filter_=None):

        """
            Get a list of locations for a dataset, using the dataset's ID.
        :param id_: the ID of the dataset
        :param filter_: 
        :return: 
        """
        return LocationsController(self.auth, filter_).get_resource('for', ['dataset', id_, 'after'])

    def get_bounds_for_programme(self, id_, filter_=None):

        """
            Get a list of bounds for a programme, using the programme's ID.
        :param id_: the ID of the programme.
        :param filter_: 
        :return: 
        """
        return BoundsController(self.auth, filter_).get_resource('for', dict(programme=id_))

    def get_bounds_for_region(self, id_, filter_=None):

        """
            Get a list of bounds for a region, using the region's ID.
        :param id_: the ID of the region.
        :param filter_: 
        :return: 
        """
        return BoundsController(self.auth, filter_).get_resource('for', dict(region=id_))

    def get_bounds_for_project(self, id_, filter_=None):

        """
            Get a list of bounds for a project.
        :param id_: the ID of the project to get bounds for.
        :param filter_: 
        :return: 
        """
        return BoundsController(self.auth, filter_).get_resource('for', dict(project=id_))

    def get_bounds_for_dataset(self, id_, filter_=None):

        """
            Get a list of bounds for a dataset, using the dataset's ID.
        :param id_: the ID of the dataset
        :param filter_: 
        :return: 
        """
        return BoundsController(self.auth, filter_).get_resource('for', dict(dataset=id_))

    def get_before_fatalities(self, id_, dataset_id, filter_=None):
        # TODO: Provjerit

        """
            Fetches fatalities for a specified location. You must specify the location ID,
            and the ID of the dataset it belongs to.
        :param id_: the location ID of the fatalities
        :param dataset_id: the ID of the dataset the fatality row is in.
        :param filter_: 
        :return: 
        """
        return FatalitiesController(self.auth, filter_).get_resource(id_, [dataset_id, 'before'])

    def get_after_fatalities(self, id_, dataset_id, filter_=None):
        # TODO: Provjerit

        """
            Fetches fatalities for a specified location. You must specify the location ID,
            and the ID of the dataset it belongs to.
        :param id_: the location ID of the fatalities
        :param dataset_id: the ID of the dataset the fatality row is in.
        :param filter_: 
        :return: 
        """
        return FatalitiesController(self.auth, filter_).get_resource(id_, [dataset_id, 'after'])

    def get_before_fatalities_for_programme(self, id_, filter_=None):

        """
            Get a list of "before" fatalities for a programme, using the programme's ID.
        :param id_: the ID of the programme.
        :param filter_: 
        :return: 
        """
        return FatalitiesController(self.auth, filter_).get_resource('for', ['programme', id_, 'before'])

    def get_after_fatalities_for_programme(self, id_, filter_=None):

        """
            Get a list of "after" fatalities for a programme, using the programme's ID.
        :param id_: the ID of the programme.
        :param filter_: 
        :return: 
        """
        return FatalitiesController(self.auth, filter_).get_resource('for', ['programme', id_, 'after'])

    def get_before_fatalities_for_region(self, id_, filter_=None):

        """
            Get a list of "before" fatalities for a region, using the region's ID.
        :param id_: the ID of the region.
        :param filter_: 
        :return: 
        """
        return FatalitiesController(self.auth, filter_).get_resource('for', ['region', id_, 'before'])

    def get_after_fatalities_for_region(self, id_, filter_=None):

        """
            Get a list of "after" fatalities for a region, using the region's ID.
        :param id_: the ID of the region.
        :param filter_: 
        :return: 
        """
        return FatalitiesController(self.auth, filter_).get_resource('for', ['region', id_, 'after'])

    def get_before_fatalities_for_project(self, id_, filter_=None):

        """
            Get a list of "before" fatalities for a project, using the project's ID.
        :param id_: the ID of the project we are getting fatalities for.
        :param filter_: 
        :return: 
        """
        return FatalitiesController(self.auth, filter_).get_resource('for', ['project', id_, 'before'])

    def get_after_fatalities_for_project(self, id_, filter_=None):

        """
            Get a list of "after" fatalities for a project, using the project's ID.
        :param id_: the ID of the project we are getting fatalities for.
        :param filter_: 
        :return: 
        """
        return FatalitiesController(self.auth, filter_).get_resource('for', ['project', id_, 'after'])

    def get_before_fatalities_for_dataset(self, id_, filter_=None):

        """
            Get a list of the before fatalities for a dataset, using the dataset's ID.
        :param id_: the ID of the dataset we are getting fatalities for.
        :param filter_: 
        :return: 
        """
        return FatalitiesController(self.auth, filter_).get_resource('for', ['dataset', id_, 'before'])

    def get_after_fatalities_for_dataset(self, id_, filter_=None):

        """
            Get a list of the after fatalities for a dataset, using the dataset's ID.
        :param id_: the ID of the dataset we are getting fatalities for.
        :param filter_: 
        :return: 
        """
        return FatalitiesController(self.auth, filter_).get_resource('for', ['dataset', id_, 'after'])

    def get_before_star_ratings(self, id_, dataset_id, filter_=None):

        """
            Fetches a before countermeasures star rating for a specified location. You must specify the 
            location ID, and the ID of the dataset it belongs to.
        :param id_: 
        :param dataset_id: 
        :param filter_: 
        :return: 
        """
        return StarRatingsController(self.auth).get_before_star_ratings(id_, dataset_id, filter_)

    def get_before_star_ratings_for_programme(self, id_, filter_=None):

        """
            Get a list of star ratings for a programme, using the programme's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return StarRatingsController(self.auth, filter_).get_resource('for', ['programme', id_, 'before'])

    def get_before_star_ratings_for_region(self, id_, filter_=None):

        """
            Get a list of star ratings for a region, using the region's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return StarRatingsController(self.auth, filter_).get_resource('for', ['region', id_, 'before'])

    def get_before_star_ratings_for_project(self, id_, filter_=None):

        """
            Get a list of star ratings for a project, using the project's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return StarRatingsController(self.auth, filter_).get_resource('for', ['project', id_, 'before'])

    def get_before_star_ratings_for_dataset(self, id_, filter_=None):

        """
            Get a list of star ratings for a dataset, using the dataset's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return StarRatingsController(self.auth, filter_).get_before_star_ratings_for_dataset(id_, filter_)

    def get_after_star_ratings(self, id_, dataset_id, filter_=None):

        """
            Fetches an after countermeasures star rating for a specified location. You must specify the 
            location ID, and the ID of the dataset it belongs to.
        :param id_: 
        :param dataset_id: 
        :param filter_: 
        :return: 
        """
        return StarRatingsController(self.auth, filter_).get_resource(id_, dict(after=dataset_id))

    def get_after_star_ratings_for_programme(self, id_, filter_=None):

        """
            Get a list of star ratings for a programme, using the programme's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return StarRatingsController(self.auth, filter_).get_resource('for', ['programme', id_, 'after'])

    def get_after_star_ratings_for_region(self, id_, filter_=None):

        """
            Get a list of star ratings for a region, using the region's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return StarRatingsController(self.auth, filter_).get_resource('for', ['region', id_, 'after'])

    def get_after_star_ratings_for_project(self, id_, filter_=None):

        """
            Get a list of star ratings for a project, using the project's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return StarRatingsController(self.auth, filter_).get_resource('for', ['project', id_, 'after'])

    def get_after_star_ratings_for_dataset(self, id_, filter_=None):

        """
            Get a list of star ratings for a dataset, using the dataset's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return StarRatingsController(self.auth, filter_).get_after_star_ratings_for_dataset(id_, filter_)

    def get_data(self, id_, dataset_id, filter_=None):

        """
            Fetches a data for a specified location. You must specify the location ID,
            and the ID of the dataset it belongs to.
        :param id_: 
        :param dataset_id: 
        :param filter_: 
        :return: 
        """
        return DataController(self.auth, filter_).get_resource(id_, dataset_id)

    def add_data(self, data, dataset_id):

        """
            Adds a set of data for the specified location. $data should be an array 
            list of name-value pairs, where the name matches the relevant field in the database.
            dataset_id should be the ID of the dataset the data are associated with.
        :param data: 
        :param dataset_id: 
        """
        raise Exception

    def update_data(self, id_, data, dataset_id):

        """
            Updates a set of data for the specified location. data should be an 
            array list of name-value pairs, where the name matches the relevant field in the database.
            dataset_id should be the ID of the dataset the data are associated with. $id
            should be the ID of the set of data to update.
        :param id_: 
        :param data: 
        :param dataset_id: 
        """
        raise Exception

    def replace_data(self, id_, data, dataset_):

        """
            Replaces a set of data for the specified location. $data should be an 
            array list of name-value pairs, where the name matches the relevant field in the database.
            dataset_id should be the ID of the dataset the data are associated with. $id
            should be the ID of the set of data to replace.
        :param id_: 
        :param data: 
        :param dataset_: 
        """
        raise Exception

    def delete_data(self, id_, dataset_id):

        """
            Deletes a set of data from the system, using the set of data's ID.
        :param id_: 
        :param dataset_id: 
        """
        raise Exception

    def import_data(self, dataset_id, url):

        """
            Imports a CSV file from the specified url. The CSV file is expected to have a header
            row that will be ignored.
        :param dataset_id: the ID of the dataset we wish to import for.
        :param url: the url to the CSV file we wish to import. Temporary pre-signed s3 urls recommended.
        :return: 
        """
        return DataController(self.auth).import_data(dataset_id, url)

    def get_data_for_programme(self, id_, filter_=None):

        """
            Get a list of data for a programme, using the programme's ID. 
        :param id_: 
        :param filter_: 
        :return: 
        """
        return DataController(self.auth, filter_).get_resource('for', dict(programme=id_))

    def get_data_for_region(self, id_, filter_=None):

        """
            Get a list of data for a region, using the region's ID. 
        :param id_: 
        :param filter_: 
        :return: 
        """
        return DataController(self.auth, filter_).get_resource('for', dict(region=id_))

    def get_data_for_project(self, id_, filter_=None):

        """
            Get a list of data for a project, using the project's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return DataController(self.auth, filter_).get_resource('for', dict(project=id_))

    def get_data_for_dataset(self, id_, filter_=None):

        """
            Get a list of data for a dataset, using the dataset's ID.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return DataController(self.auth, filter_).get_resource('for', dict(dataset=id_))

    def get_countries(self, id_=None, filter_=None):

        """
            Fetches a list of all of the countries in the system. If you specify an ID, that country
            will be returned to you.
        :param id_: 
        :param filter_: 
        :return: 
        """
        return CountriesController(self.auth, filter_).get_resource(id_)

    def get_permissions(self, filter_=None):

        """
            Fetches the permissions for the user. If this is called on an app object, it fetches
            the permissions for the app.
        :param filter_: 
        :return: 
        """
        return PermissionsController(self.auth, filter_).get_resource()

    def get_star_rating_results_summary_for_programme(self, id_, filter_=None):
        """
            Get a list of star rating results summary for a programme, using the programme's ID.
        :param id_: 
        :param filter_: star_rating_results_summary
        """
        return StarRatingResultsSummaryController(self.auth, filter_).get_resource('for', dict(programme=id_))

    def get_star_rating_results_summary_for_region(self, id_, filter_=None):
        """
            Get a list of star rating results summary for a region, using the region's ID.
        :param id_: 
        :param filter_: 
        """
        return StarRatingResultsSummaryController(self.auth, filter_).get_resource('for', dict(region=id_))

    def get_star_rating_results_summary_for_project(self, id_, filter_=None):
        """
            Get a list of star rating results summary for a project, using the project's ID.
        :param id_: 
        :param filter_: 
        """
        return StarRatingResultsSummaryController(self.auth, filter_).get_resource('for', dict(project=id_))

    def get_star_rating_results_summary_for_dataset(self, id_, filter_=None):
        """
            Get a list of star rating results summary for a dataset, using the dataset's ID.
        :param id_: 
        :param filter_: 
        """
        return StarRatingResultsSummaryController(self.auth, filter_).get_resource('for', dict(dataset=id_))

    def get_report_filter(self, id_, filter_=None):
        """
            Get a report filter by ID.
        :param id_: report_filters
        :param filter_: 
        """
        return ReportFiltersController(self.auth, filter_).get_resource(id_)

    def add_report_filter(self, filter_json):
        """
            Creates a new report filter using the supplied filter json
        :param filter_json: 
        """
        return ReportFiltersController(self.auth).post_resource(dict(filter_json=filter_json))

    def get_map_star_ratings_for_dataset(self, id_, filter_=None):

        """
            Gets before and after star ratings for a dataset, plus geo information
        :param id_:
        :param filter_:
        :return:
        """
        return DriverController(self.auth, filter_).get_map_star_ratings_for_dataset(id_, filter_)

    def get_modal_info_for_dataset(self, id_, latitude, longitude, language, filter_=None):
        #language='en-gb'

        """
            Gets before and after star ratings for a dataset, plus geo information, with attribute information
            in the specified language
        :param id_:
        :param latitude:
        :param longitude:
        :param language:
        :param filter_:
        :return:
        """
        return DriverController(self.auth, filter_).get_modal_info_for_dataset(id_, latitude, longitude, language, filter_)

    def get_all_datasets(self, filter_=None):

        """
            Gets all the datasets for a user, including 'Final Published' ones
        :return:
        """
        return DriverController(self.auth, filter_).get_all_datasets(filter_)
