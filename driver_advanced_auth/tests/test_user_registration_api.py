import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'DRIVER.settings')

# Initialize Django
django.setup()
import pytest
from django.urls import reverse
from rest_framework.test import APIClient
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST
from django.contrib.auth.models import User, Group



# pytest path/to/test_user_registration_api.py


@pytest.mark.django_db
class TestUserRegistrationAPI:

    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def create_user(self):
        def _create_user(username, email, is_active=True):
            return User.objects.create(username=username, email=email, is_active=is_active)
        return _create_user

    @pytest.fixture
    def create_group(self):
        def _create_group(name):
            return Group.objects.create(name=name)
        return _create_group

    def test_get_active_users(self, api_client, create_user):
        """
        Test GET /api/registration/ for listing active users.
        """
        # Arrange
        user1 = create_user(username="valid_user", email="valid@aventior.com")
        create_user(username="invalid_user", email="invalid@aventior.com", is_active=False)

        url = reverse('api/registration/')  # Replace 'registration' with the correct name of your endpoint.

        # Act
        response = api_client.get(url)

        # Assert
        assert response.status_code == HTTP_200_OK
        assert len(response.data[0]["data"]) == 1
        assert response.data[0]["data"][0][0]["username"] == user1.username
        assert response.data[0]["message"] == "success"
        assert response.data[0]["status"] is True

    def test_post_user_registration_success(self, api_client, create_group):
        """
        Test POST /api/registration/ for successful user registration.
        """
        # Arrange
        group = create_group(name="Tech Analyst")
        url = reverse('api/registration/')
        data = {
            "username": "aventior",
            "email": "aventior.user@aventior.com",
            "password": "Aventior@2024"
        }

        # Act
        response = api_client.post(url, data)

        # Assert
        assert response.status_code == HTTP_200_OK
        assert "token" in response.data[0]
        assert response.data[0]["username"] == "aventior"
        assert response.data[0]["email"] == "aventior.user@aventior.com"

        # Verify user is assigned to the group
        user = User.objects.get(username="aventior")
        assert group in user.groups.all()

    def test_post_user_registration_invalid_data(self, api_client):
        """
        Test POST /api/registration/ with invalid data.
        """
        # Arrange
        url = reverse('api/registration/')
        # with missing username and email
        data = {
            "username": "",
            "email": "",
            "password": "Aventior@2024"
        }

        # Act
        response = api_client.post(url, data)

        # Assert
        assert response.status_code == HTTP_400_BAD_REQUEST
        assert "username" in response.data[0]
        assert "email" in response.data[0]
