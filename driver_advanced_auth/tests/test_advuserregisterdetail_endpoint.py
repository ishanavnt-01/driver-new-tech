from rest_framework.test import APITestCase, APIClient
from rest_framework import status
from driver_advanced_auth.models import UserDetail
from driver_advanced_auth.serializers import AdvUserSerializer

# api/registration/
# python manage.py test driver_advanced_auth.tests.test_advuserregisterdetail_endpoint --keepdb

from rest_framework.test import APITestCase
from rest_framework import status
from django.contrib.auth.models import User

# class UserRegistrationAPITestCase(APITestCase):
#
#     def setUp(self):
#         # Create a sample user for testing GET
#         self.user = User.objects.create_user(username='djagotest1', password='djagotest1', email='djago.test1@aventior.com', is_active=True)
#         self.user.save()
#         self.valid_user_data = {
#             'username': 'djagotest',
#             'password': 'djagotest',
#             'email': 'djago.test@aventior.com',
#             'is_staff': False,
#             'is_superuser': False,
#         }
#         self.invalid_user_data = {
#             'username': '',  # username not given to test error
#             'password': 'short',
#         }
#
#     def test_get_users(self):
#         response = self.client.get('/api/registration/')
#         self.assertEqual(response.status_code, status.HTTP_200_OK)
#         self.assertIn('data', response.json()[0])
#         self.assertEqual(response.json()[0]['message'], 'success')
#         self.assertTrue(response.json()[0]['status'])
#
#     def test_post_valid_user_registration(self):
#         response = self.client.post('/api/registration/', data=self.valid_user_data)
#         self.assertEqual(response.status_code, 200)
#         self.assertIn('username', response.data[0])
#         self.assertTrue(User.objects.filter(username='djagotest').exists())
#
#     def test_post_invalid_user_registration(self):
#         response = self.client.post('/api/registration/', data=self.invalid_user_data)
#         self.assertEqual(response.status_code, 400)
#         self.assertIn('username', response.data[0])  # error message will be returned {'username': [ErrorDetail(string='This field may not be blank.', code='blank')], 'email': [ErrorDetail(string='This field is required.', code='required')]}
#         self.assertFalse(User.objects.filter(username='').exists())


# auth-api/adv-registration/
class AdvUserRegisterDetailAPITestCase(APITestCase):

    def setUp(self):
        # Set up the API client
        self.client = APIClient()
        self.url = 'auth-api/adv-registration/'
        self.auth_user = User.objects.create_user(username='djagotest1', password='djagotest1',
                                             email='djago.test1@aventior.com', is_active=True)

        self.user_detail = UserDetail.objects.create(
            first_name='djagotest1',
            email='djago.test1@aventior.com',
            user_id = self.auth_user.id
        )

    def test_get_user_detail_success(self):
        """Test retrieving user detail by ID."""
        response = self.client.get(f'{self.url}{self.auth_user.id}/', format='json')

        # Serialize the object manually to compare
        expected_data = AdvUserSerializer(self.user_detail).data

        # Assertions
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, [expected_data])

    def test_get_user_detail_not_found(self):
        """Test retrieving a non-existent user detail."""
        response = self.client.get(f'{self.url}9999/', format='json')

        # Assertions
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    # def test_update_user_detail_success(self):
    #     """Test updating user detail successfully."""
    #     update_data = {
    #         "username": "Updated Test User",
    #         "email": "updateduser@example.com"
    #     }
    #     response = self.client.put(f'auth-api/adv-registration-detail?id=/{self.user_detail.id}/', update_data, format='json')
    #
    #     # Refresh the instance from the database
    #     self.user_detail.refresh_from_db()
    #
    #     # Assertions
    #     self.assertEqual(response.status_code, status.HTTP_200_OK)
    #     self.assertEqual(self.user_detail.name, update_data['name'])
    #     self.assertEqual(self.user_detail.email, update_data['email'])

    def test_update_user_detail_not_found(self):
        """Test updating a non-existent user detail."""
        update_data = {
            "name": "Non-existent User",
            "email": "nonexistent@example.com"
        }
        response = self.client.put(f'auth-api/adv-registration-detail/9999/', update_data, format='json')

        # Assertions
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
