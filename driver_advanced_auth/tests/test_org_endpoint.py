# from rest_framework.test import APITestCase, APIClient
# from rest_framework import status
# from grout.models import Boundary
# from grout.serializers import BoundarySerializer
#
# class GetCountryTestCase(APITestCase):
#
#     def setUp(self):
#         # Set up the API client
#         self.client = APIClient()
#         self.url = 'auth-api/getcountry/'
#
#         # Create sample Boundary objects
#         self.boundary1 = Boundary.objects.create(label='Test_Samoa')
#         self.boundary2 = Boundary.objects.create(name='Test_Philippines')
#
#     def test_get_countries_success(self):
#         """Test retrieving all countries."""
#         response = self.client.get(self.url, format='json')
#
#         # Serialize the data manually to compare
#         expected_data = BoundarySerializer([self.boundary1, self.boundary2], many=True).data
#
#         # Assertions
#         self.assertEqual(response.status_code, status.HTTP_200_OK)
#         self.assertEqual(response.data['data'], expected_data)


from rest_framework.test import APITestCase, APIClient
from rest_framework import status
from driver_advanced_auth.models import Organization, CountryInfo
from driver_advanced_auth.serializers import OrganizationSerializer
from driver_irap.modules.irap_vida.models.country import Country
from grout.models import Boundary, BoundaryPolygon
import logging

logger = logging.getLogger()

class OrganizationListTestCase(APITestCase):

    def setUp(self):
        # Set up the API client
        self.client = APIClient()
        self.url = 'auth-api/org/'



        country_obj = Boundary.objects.filter().last()
        # for country_obj in country_objs:
        #     logger.info(f'---country---{country_obj}------')

        # Create sample Organization objects
        self.organization1 = Organization.objects.create(name='Falelatai & Samatau', country_id="d127255b-4d99-4d55-9c8f-47d625ee5875", region_id="3fc4b179-ab01-47f1-8a37-202e7c5b6dff")
        self.organization2 = Organization.objects.create(name='Vaimauga East', country_id="d127255b-4d99-4d55-9c8f-47d625ee5875", region_id="22bac565-9788-4450-89af-a6f058b927ed")

    # def test_create_organization_success(self):
    #     """Test creating a new organization successfully."""
    #     data =  {
    #         "country": "d127255b-4d99-4d55-9c8f-47d625ee5875",
    #         "region": "1038a1bb-7c0a-4253-8949-f35760bfd309",
    #         "name": "Falealili"
    #     }
    #     response = self.client.post(self.url, data, format='json')
    #
    #     # Assertions
    #     self.assertEqual(response.status_code, status.HTTP_200_OK)
    #     self.assertEqual(response.data["message"], "Details added successfully")
    #     self.assertEqual(response.data["status"], "true")

    # def test_create_organization_duplicate(self):
    #     """Test creating a duplicate organization."""
    #     data =  {
    #         "country": "d127255b-4d99-4d55-9c8f-47d625ee5875",
    #         "region": "1038a1bb-7c0a-4253-8949-f35760bfd309",
    #         "name": "Falealili"
    #     }
    #     response = self.client.post(self.url, data, format='json')
    #
    #     # Assertions
    #     self.assertEqual(response.status_code, status.HTTP_200_OK)
    #     self.assertEqual(response.data["message"], "Organization already exists. Please try with another Organization name")
    #     self.assertEqual(response.data["status"], "false")

    def test_list_organizations(self):
        """Test retrieving all organizations."""

        country_objs = Boundary.objects.all()
        print('-------country_objs--', country_objs)
        logger.info(f'---country---{country_objs}------')

        for country_obj in country_objs:
            logger.info(f'---country---{country_obj}------')

        response = self.client.get(self.url, format='json')

        # Serialize the data manually to compare
        expected_data = OrganizationSerializer([self.organization1, self.organization2], many=True).data

        # Assertions
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["data"], expected_data)
        self.assertEqual(response.data["status"], "true")
