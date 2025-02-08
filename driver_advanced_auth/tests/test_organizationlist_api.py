import django
django.setup()

import pytest
from django.urls import reverse
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST

from driver_advanced_auth.models import Organization
from django.contrib.auth.models import User

@pytest.mark.django_db
class TestOrganizationListAPI:
    """
    Test cases for the OrganizationList API endpoint.
    """

    @pytest.fixture
    def api_client(self, client):
        """
        Fixture to provide authenticated API client.
        """
        user = User.objects.create_user(username="test", password="test")
        client.force_login(user)
        return client


    def test_create_organization_success(self, api_client, create_country):
        """
        Test POST auth-api/org/ for successful organization creation.
        1. Verifies successful organization creation.
        2. Confirms organization saved in database.
        """
        # Arrange
        url = reverse('auth-api/org/')
        data = {
            "name": "Sagaga le Usoga Organization 2",
            "country": "b7b6c8a4-8594-4f6a-9821-935c17b76921",
            "region": "a72e0c76-ccb7-49bf-bae0-828300759b66"
        }

        # Act
        response = api_client.post(url, data, content_type='application/json')

        # Assert
        assert response.status_code == HTTP_200_OK
        response_data = response.json()
        assert response_data["message"] == "Details added successfully"
        assert response_data["status"] == "true"
        assert Organization.objects.filter(name="TechCorp", country="b7b6c8a4-8594-4f6a-9821-935c17b76921",
                                           region="a72e0c76-ccb7-49bf-bae0-828300759b66").exists()

    def test_create_organization_duplicate(self, api_client):
        """
        Test POST auth-api/org/ for duplicate organization.
        1. Duplicate organization behaviour test.
        2. Confirms error message should be returned on duplicate organization creation.
        """
        # Arrange
        url = reverse('auth-api/org/')
        Organization.objects.create(name="TechCorp", country="b7b6c8a4-8594-4f6a-9821-935c17b76921")
        data = {
            "name": "Sagaga le Usoga Organization",
            "country": "b7b6c8a4-8594-4f6a-9821-935c17b76921",
            "region": "a72e0c76-ccb7-49bf-bae0-828300759b66"
        }

        # Act
        response = api_client.post(url, data, content_type='application/json')

        # Assert
        assert response.status_code == HTTP_200_OK
        response_data = response.json()
        assert response_data["message"] == "Organization already exists. Please try with another Organization name"
        assert response_data["status"] == "false"

    def test_create_organization_invalid_data(self, api_client):
        """
        Test POST auth-api/org/ with invalid data.
        1. Tests Validation error on invalid data.
        """
        # Arrange
        url = reverse('auth-api/org/')
        data = {
            "name": "invalid data",
            "country": "12345-1234-1234-1234-12354654788799",
            "region": "12345-1234-1234-1234-12354654788799"
        }

        # Act
        response = api_client.post(url, data, content_type='application/json')

        # Assert
        assert response.status_code == HTTP_400_BAD_REQUEST
        response_data = response.json()
        assert response_data["status"] == "false"
        assert "name" in response_data["message"]
        assert "country" in response_data["message"]
        assert "region" in response_data["message"]

    def test_get_organizations(self, api_client):
        """
        Test GET auth-api/org/ for retrieving organization list.
        1. Verifies GET endpoint returns list of all the organizations.
        """
        # Arrange
        url = reverse('auth-api/org/')

        # Act
        response = api_client.get(url)

        # Assert
        assert response.status_code == HTTP_200_OK
        response_data = response.json()
        assert response_data["status"] == "true"

