import pytest
from django.urls import reverse
from rest_framework.status import HTTP_201_CREATED, HTTP_400_BAD_REQUEST
from django.contrib.auth.models import Group, User



@pytest.mark.django_db
class TestUserListAPI:
    """
    Test cases for the auth-api/newusers/ endpoint.
    """

    @pytest.fixture
    def api_client(self, client):
        """
        Fixture to provide API client.
        """
        return client

    @pytest.fixture
    def create_group(self):
        """
        Fixture to create a group for testing.
        """
        def _create_group(name):
            return Group.objects.create(name=name)
        return _create_group

    def test_create_user_success(self, api_client, create_group):
        """
        Test POST auth-api/newusers/ for successful user creation.
        """
        # Arrange
        url = reverse('auth-api/newusers/')
        data = {
            "first_name": "John",
            "last_name": "Doe",
            "username": "johndoe",
            "email": "johndoe@aventior.com",
            "password": "secure_password",
            "groups": ["Tech Analyst"]
        }
        create_group(name="Tech Analyst")

        # Act
        response = api_client.post(url, data, content_type='application/json')

        # Assert
        assert response.status_code == HTTP_201_CREATED
        response_data = response.json()
        assert response_data["first_name"] == "John"
        assert response_data["last_name"] == "Doe"
        assert response_data["username"] == "johndoe"
        assert response_data["email"] == "johndoe@example.com"
        assert "groups" in response_data
        assert "User Group" in response_data["groups"]

    def test_create_user_duplicate_username(self, api_client):
        """
        Test POST /auth-api/newusers/ for duplicate username.
        """
        # Arrange
        url = reverse('auth-api/newusers/')
        User.objects.create_user(username="johndoe", email="johndoe@example.com", password="secure_password")
        data = {
            "first_name": "Jane",
            "last_name": "Doe",
            "username": "johndoe",
            "email": "janedoe@example.com",
            "password": "another_password"
        }

        # Act
        response = api_client.post(url, data, content_type='application/json')

        # Assert
        assert response.status_code == HTTP_400_BAD_REQUEST
        assert "username" in response.json()

    def test_create_user_invalid_email(self, api_client):
        """
        Test POST /auth-api/newusers/ with invalid email.
        """
        # Arrange
        url = reverse('/auth-api/newusers/')  # Replace 'user-list' with your endpoint's URL name if different
        data = {
            "first_name": "Jane",
            "last_name": "Doe",
            "username": "janedoe",
            "email": "invalid-email",  # Invalid email format
            "password": "secure_password"
        }

        # Act
        response = api_client.post(url, data, content_type='application/json')

        # Assert
        assert response.status_code == HTTP_400_BAD_REQUEST
        assert "email" in response.json()

