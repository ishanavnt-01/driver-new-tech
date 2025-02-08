import hashlib
import os
import base64
import csv
import json
import logging
import datetime
import uuid
import requests
from django.http import Http404
from driver_advanced_auth.models import UserDetail, CountryInfo, LanguageDetail, PasswordHistory
from driver_advanced_auth.serializers import (RequestSerializer, CitySerializer, OrganizationSerializer,
                                              ContentTypeSerializer, PermissionSerializers, AssociateGroupSerializer,
                                              GroupSerializer, GroupSerializerNew, AdvAuthSerializer, UserSerializer,
                                              AdvUserSerializer, DetailsSerializer, ApproveRejectRequestSerializer,
                                              RejectRequestSerializer, TokenSerializer, GoogleUserSerializer,
                                              CountryInfoSerializer, LanguageDetailSerializer) # SendRoleRequestSerializer
from data.serializers import DriverRecordCopySerializer, WeatherInfoSerializer, BaseDriverRecordSerializer
from rest_framework.views import APIView
from rest_framework.response import Response
from django.contrib.auth.hashers import make_password
from django.contrib.auth.models import Group, User, Permission
from driver_advanced_auth.models import City, Organization, Groupdetail, SendRoleRequest
from django.http import JsonResponse
from grout.models import BoundaryPolygon, Boundary, RecordType, RecordSchema
from grout.serializers import BoundarySerializer, BoundaryPolygonSerializer
from django.db import connections
from django.contrib.contenttypes.models import ContentType
from rest_framework.parsers import JSONParser
from rest_framework.permissions import AllowAny
from rest_framework.authtoken.models import Token
from rest_framework.authtoken.views import ObtainAuthToken
from django.conf import settings
from oauth2client import client, crypt
from django.contrib.auth import authenticate
from rest_framework import status, viewsets
from django.core.mail import BadHeaderError, EmailMultiAlternatives
from data.models import DriverRecordCopy, WeatherInfo, DriverRecord, DuplicateDistanceConfig, IrapDetail, KeyDetail
from grout.models import Record
from rest_framework.permissions import IsAuthenticated
from django.core.mail import send_mail
from django.db.models.expressions import RawSQL
from django.db.models import Q
from django.db import transaction
from datetime import datetime as strf_date
from rest_framework import serializers
from grout.pagination import OptionalLimitOffsetPagination
from data.filters import CountryInfoFilter, LanguageDetailFilter
from rest_framework.decorators import api_view
from rest_framework.decorators import authentication_classes, permission_classes
from rest_framework.authentication import TokenAuthentication
from django.utils import timezone
from datetime import timedelta

logger = logging.getLogger(__name__)

# Create new user (Sign up form) and get all user
class UserList(APIView):

    def post(self, request, format=None):
        serializer = UserSerializer(data=request.data, context={'request': request})
        if serializer.is_valid():
            password = make_password(self.request.data['password'])
            serializer.save(password=password)
            id = serializer.data['id']
            return Response(serializer.data, status=status.HTTP_201_CREATED)

        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


# Edit auth users and their permissions
class UserById(APIView):

    def get_object(self, pk):
        try:
            return User.objects.get(pk=pk)
        except User.DoesNotExist:
            raise Http404

    def put(self, request, pk, format=None):

        getprivatekey = self.get_object(pk)
        serializer = User(getprivatekey, data=request.data, context={'request': request})
        password = make_password(self.request.data['password'])

        #   serializer.save(password=password,user_permissions=[28])
        if serializer.is_valid():
            serializer.save(password=password, )
            return Response(serializer.data, status=status.HTTP_201_CREATED)

        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def get(self, request, pk):
        queryset = User.objects.filter(id=pk)
        serializeJson = UserSerializer(queryset, many=True, context={'request': request})
        return Response(serializeJson.data[0])


# add and get City
class CityList(APIView):

    def post(self, request, format=None):
        serializer = CitySerializer(data=request.data)
        if serializer.is_valid():
            name = request.data.get('name')
            country_uuid = request.data.get('country_id')
            region_uuid = request.data.get('region_id')
            if not City.objects.filter(name=name, country=country_uuid, region=region_uuid).exists():
                City.objects.create(name=name, country_id=country_uuid, region_id=region_uuid)
                return Response({"message": "Details added successfully", "status": "true"}, status=200)
            else:
                return Response(
                    {"message": "City already exists. Please try with another City name", "status": "false"},
                    status=200)
        return Response({"message": serializer.errors, "status": "false"}, status=400)

    def get(self, request):
        queryset = City.objects.all()
        serializer = CitySerializer(queryset, many=True)
        return Response({"data": serializer.data, "status": "true"}, status=200)


# get_by_id, update, and delete City
class CityDetailList(APIView):

    def get_object(self, id):
        try:
            return City.objects.get(id=id)
        except City.DoesNotExist:
            raise Http404

    def get(self, request, id=None):
        instance = self.get_object(id)
        serializer = CitySerializer(instance)
        return Response({"data": serializer.data, "status": "true"}, status=200)

    def put(self, request, id=None):
        data = request.data
        instance = self.get_object(id)
        serializer = CitySerializer(instance, data=data)
        if serializer.is_valid():
            name = request.data.get('name')
            country_id = request.data.get('country_id')
            region_id = request.data.get('region_id')
            if not City.objects.filter(name=name, country=country_id, region=region_id).exclude(name=instance.name,
                                                                                                country=instance.country.uuid,
                                                                                                region=instance.region.uuid).exists():
                serializer.save()
                return Response({"message": "Details updated successfully", "status": "true"}, status=200)
            return Response({"message": "City already exists. Please try with another City name", "status": "false"},
                            status=200)
        return Response({"message": "error", "status": "false"}, status=400)

    def delete(self, request, id=None):
        instance = self.get_object(id)
        instance.delete()
        return Response([{"message": "Details deleted successfully", "status": "true"}], status=200)


# add and get organization
class OrganizationList(APIView):
    """Viewset for adding and list out all organizations"""
    permissions_classes = [IsAuthenticated, ]

    def post(self, request, format=None):
        serializer = OrganizationSerializer(data=request.data)
        if serializer.is_valid():
            #      serializer.save()
            name = request.data.get('name')
            country_id = request.data.get('country')
            region_id = request.data.get('region')
            if not Organization.objects.filter(name=name, country=country_id, region=region_id).exists():
                Organization.objects.create(name=name, country_id=country_id, region_id=region_id)
                return Response({"message": "Details added successfully", "status": "true"}, status=200)
            return Response({"message": "Organization already exists. Please try with another Organization name",
                             "status": "false"}, status=200)

        return Response({"message": serializer.errors, "status": "false"}, status=400)

    def get(self, request):
        queryset = Organization.objects.all().order_by('name')
        serializer = OrganizationSerializer(queryset, many=True)
        return Response({"data": serializer.data, "status": "true"}, status=200)


# get_by_id, update, and delete Organisation
class OrganisationDetailList(APIView):
    """Viewset for update/delete/get selected organization"""

    def get_object(self, id):
        try:
            return Organization.objects.get(id=id)
        except Organization.DoesNotExist as e:
            raise Http404

    def get(self, request, id=None):
        org_instance = self.get_object(id)
        serializer = OrganizationSerializer(org_instance)
        return Response({"data": serializer.data, "message": "success", "status": "true"}, status=200)

    def put(self, request, id=None):
        data = request.data
        org_instance = self.get_object(id)
        serializer = OrganizationSerializer(org_instance, data=data)
        if serializer.is_valid():
            name = request.data.get('name')
            country_id = request.data.get('country')
            region_id = request.data.get('region')
            if not Organization.objects.filter(name=name, region=region_id).exclude(
                    name=org_instance.name, region=org_instance.region.uuid).exists():
                serializer.save()
                return Response({"message": "Details updated successfully", "status": "true"}, status=200)
            return Response({"message": "Organization already exists. Please try with another Organization name",
                             "status": "false"}, status=200)
        return Response({"message": "error", "status": "false"}, status=400)

    def delete(self, request, id=None):
        org_instance = self.get_object(id)
        org_instance.delete()
        return Response([{"message": "Details deleted successfully", "status": "true"}], status=200)


# get region from boundrypolygon
class GetRegion(APIView):
    """Viewset to get list of regions from boundarypolygon"""

    def get(self, request):
        xdata = []
        data = {}
        boundary_obj = Boundary.objects.get(label="Regions")
        queryset = BoundaryPolygon.objects.filter(boundary_id=boundary_obj.uuid).order_by(
            RawSQL("data->>%s", ("region",)))
        serializer = BoundaryPolygonSerializer(queryset, many=True)
        allfeatures = serializer.data["features"]
        for item in allfeatures:
            xdata.append({"uuid": item["id"], "region": item["properties"]["data"]})

        return Response([{"result": xdata, "message": "success", "status": True}], status=200)


class GetCities(APIView):
    """Viewset to get list of cities from boundarypolygon"""
    permissions_classes = [IsAuthenticated, ]

    def get(self, request):
        xdata = []
        data = {}
        boundary_obj = Boundary.objects.get(label="City/Province")
        queryset = BoundaryPolygon.objects.filter(boundary_id=boundary_obj.uuid).order_by(
            RawSQL("data->>%s", ("name",)))
        serializer = BoundaryPolygonSerializer(queryset, many=True)
        allfeatures = serializer.data["features"]
        for item in allfeatures:
            xdata.append({"uuid": item["id"], "region": item["properties"]["data"]})

        return Response([{"result": xdata, "message": "success", "status": True}], status=200)


# get country from boundry(Added shapefile)
class GetCountry(APIView):

    def get(self, request):
        queryset = Boundary.objects.all()
        serializer = BoundarySerializer(queryset, many=True)
        return Response({"data": serializer.data}, status=200)


# create new Group and get all group
class GroupsList(APIView):

    def get(self, request):
        queryset = Group.objects.all().order_by('name')
        serializer = GroupSerializerNew(queryset, many=True, context={'request': request})
        return Response([{"data": serializer.data, "status": "true"}])

    def post(self, request, format=None):
        serializer = GroupSerializer(data=request.data, context={'request': request})
        name = request.data.get('name')
        if not Group.objects.filter(name=name).exists():
            if serializer.is_valid():
                serializer.save()
                return Response([{"data": serializer.data, "message": "Data saved successfully", "status": True}],
                                status=status.HTTP_201_CREATED)
            return Response([{"data": serializer.errors, "message": "error", "status": False}],
                            status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response([{"message": "Group name already exist", "status": True}])


# get_by_id, update, and delete Group
class GroupListDetail(APIView):

    def get_object(self, id):
        try:
            return Group.objects.get(id=id)
        except Group.DoesNotExist:
            raise Http404

    def get(self, request, id=None, format=None):
        instance = self.get_object(id)
        serializer = GroupSerializerNew(instance, context={'request': request})
        driver_group = Groupdetail.objects.get(group=id)
        new_dict = {'description': driver_group.description,
                    'is_admin': driver_group.is_admin
                    }
        new_dict.update(serializer.data)
        return Response([{"data": new_dict, "message": "Success", "status": True}], status=200)

    @transaction.atomic
    def put(self, request, id=None, format=None):
        data = request.data
        instance = self.get_object(id)
        driver_group = Groupdetail.objects.get(group=id)
        serializer = GroupSerializer(instance, data=data, context={'request': request})
        if serializer.is_valid():
            driver_group.name = request.data.get('name')
            driver_group.is_admin = request.data.get('is_admin')
            driver_group.description = request.data.get('description') if request.data.get('description') else 'null'
            driver_group.save()
            serializer.save()
            return Response([{"data": serializer.data, "message": "Details updated successfully", "status": True}],
                            status=200)
        return Response(serializer.errors, status=400)

    def delete(self, request, id=None):
        instance = self.get_object(id)
        group_detail_instance = Groupdetail.objects.get(group=id)
        group_detail_instance.delete()
        instance.delete()
        return Response([{"message": "Details deleted successfully", "status": "true"}], status=200)


# create new Group and get all group with description
class DriverGroup(APIView):

    def post(self, request, format=None):
        serializer = AssociateGroupSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response([{"Data": serializer.data, "message": "Data saved successfully", "status": True}],
                            status=status.HTTP_201_CREATED)
        return Response([{"Data": serializer.errors, "message": "error", "status": False}],
                        status=status.HTTP_400_BAD_REQUEST)

    def get(self, request):
        queryset = Groupdetail.objects.all()
        serializer = AssociateGroupSerializer(queryset, many=True, context={'request': request})
        return Response([{"Data": serializer.data, "message": "details updated", "status": True}], status=200)


class DriverGroupDetail(APIView):

    def get_object(self, id):
        try:
            return Groupdetail.objects.get(id=id)
        except Groupdetail.DoesNotExist:
            raise Http404

    def get(self, request, id=None):
        instance = self.get_object(id)
        serializer = AssociateGroupSerializer(instance)
        return Response([{"Data": serializer.data, "message": "details updated", "status": True}], status=200)

# Edit auth users and their permissions
class GroupById(APIView):

    def get(self, request, pk):
        queryset = Group.objects.get(id=pk)
        serializeJson = GroupSerializer(queryset, context={'request': request})
        return Response({"result": serializeJson.data})


class GetAuthUserListDetails(APIView):

    def get_object(self, id):
        try:
            return UserDetail.objects.get(id=id)
        except UserDetail.DoesNotExist:
            raise Http404

    def get(self, request, id=None):
        user_detail_obj = self.get_object(id)
        serializer = AdvAuthSerializer(user_detail_obj)
        return Response(serializer.data)

    def put(self, request, id=None):
        data = request.data
        user_detail_obj = self.get_object(id)
        serializer = AdvAuthSerializer(user_detail_obj, data=data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=200)
        return Response(serializer.errors, status=400)

    def delete(self, request, id=None):
        user_detail_obj = self.get_object(id)
        user_detail_obj.delete()
        return Response([{"message": "Details deleted successfully", "status": "true"}], status=200)


# get all contenttypes (all models list)
class GetContentTypes(APIView):

    def get(self, request):
        queryset = ContentType.objects.all()
        serializer = ContentTypeSerializer(queryset, many=True)
        return Response({"result": serializer.data})


# get contenttypes id by name (a models by model name)
class GetContentTypesbyname(APIView):

    def post(self, request):
        queryset = ContentType.objects.filter(model=request.data.get('module'))
        serializer = ContentTypeSerializer(queryset, many=True)
        return Response([{"data": serializer.data, "message": "Details saved successfully", "status": True}],
                        status=200)

    #  get all permissions
    def get(self, request):
        queryset = Permission.objects.all()
        serializer = PermissionSerializers(queryset, many=True)
        return Response([{"data": serializer.data, "message": "Success", "status": True}], status=200)


# get all permissions model (contenttypes) wise
class permissionByContentId(APIView):

    def post(self, request):
        queryset = Permission.objects.filter(content_type=request.data.get('id'))
        serializer = PermissionSerializers(queryset, many=True)
        return Response([{"data": serializer.data, "message": "Details saved successfully", "status": True}],
                        status=200)

    # # get all permissions
    # def get(self, request):
    #     queryset = Permission.objects.all()
    #     serializer = PermissionSerializers(queryset, many=True)
    #     return Response([{"data": serializer.data, "message": "Success", "status": True}], status=200)

    # get all nested data as a permissions
    def get(self, request):
        with connections['default'].cursor() as cursor:
            nested_permissions = []
            query = "select django_content_type.* from django_content_type"
            cursor.execute(query)
            desc = cursor.description
            results = [
                dict(zip([col[0] for col in desc], row))
                for row in cursor.fetchall()
            ]
            if not results:
                return Response({"result": "No data found"})
            for record in results:
                query = "select auth_permission.* from auth_permission where auth_permission.content_type_id= " + str(
                    record["id"])
                cursor.execute(query)
                desc = cursor.description
                results = [
                    dict(zip([col[0] for col in desc], row))
                    for row in cursor.fetchall()
                ]
                nested_permissions.append(
                    {"model_id": record["id"], "model_name": record["model"], "app_label": record["app_label"],
                     "permisssions": results})
        return Response({"result": nested_permissions})  # get permission by Group


class GetGroupWisePermission(APIView):

    def post(self, request):
        with connections['default'].cursor() as cursor:
            group_id = request.data.get('group_id')
            content_type_id = request.data.get('content_type_id')
            codename = request.data.get('codename')
            query = "SELECT auth_group.id, auth_group.name as group_name, driver_advanced_auth_groupdetail.id, driver_advanced_auth_groupdetail.name as group_details_name,driver_advanced_auth_groupdetail.description,driver_advanced_auth_groupdetail.group_id ,auth_group_permissions.group_id , auth_group_permissions.permission_id , auth_permission.id, auth_permission.name as permission_name, auth_permission.name, auth_permission.codename , auth_permission.content_type_id ,django_content_type.app_label, django_content_type.model FROM auth_group INNER JOIN driver_advanced_auth_groupdetail ON auth_group.id = driver_advanced_auth_groupdetail.group_id INNER JOIN auth_group_permissions ON auth_group.id = auth_group_permissions.group_id INNER JOIN auth_permission ON auth_permission.id = auth_group_permissions.permission_id INNER JOIN django_content_type ON auth_permission.content_type_id = django_content_type.id WHERE auth_group.id = " + str(
                group_id) + " and auth_permission.content_type_id = " + str(
                content_type_id) + " and auth_permission.codename =" + "'" + codename + "'"
            cursor.execute(query)
            desc = cursor.description
            results = [
                dict(zip([col[0] for col in desc], row))
                for row in cursor.fetchall()
            ]
            if not results:
                return Response({"result": "No data found"})
        return Response({"result": results})


class RequestForRole(APIView):
    """Viewset for requesting role"""
    permissions_classes = [IsAuthenticated, ]

    def post(self, request, format=None):
        serializer = RequestSerializer(data=request.data)
        user = request.data.get('user')
        city = request.data.get('city')
        region = request.data.get('region')
        adv_user = UserDetail.objects.get(user=user)
        for group in adv_user.groups.all():
            group_name = group.name
        # adv_user.city=city

        if serializer.is_valid():
            serializer.save(current_group=group_name)
            adv_user.is_role_requested = "Requested"
            to_email = adv_user.email
            adv_user.save()

            email_host_password = KeyDetail.objects.get(keyname="email_host_password").value
            subject, from_email = 'Request For Role', settings.DEFAULT_FROM_EMAIL
            send_mail(
                subject,
                'You have successfully requested for a new role!',
                from_email,
                [to_email],
                fail_silently=False,
                auth_password=email_host_password
            )
            return Response([{"data": serializer.data, "message": "success", "status": True}],
                            status=status.HTTP_201_CREATED)

        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class AcceptRoleRequest(APIView):
    """Viewset to save updated role information for perticular user"""

    permissions_classes = [IsAuthenticated, ]

    def get_object(self, user):
        try:
            return UserDetail.objects.get(user=user)
        except UserDetail.DoesNotExist:
            raise Http404

    @transaction.atomic
    def put(self, request, user=None):
        data = request.data
        user_instance = self.get_object(user)
        auth_user_instance = User.objects.get(id=user)
        to_email = auth_user_instance.email
        group = data.get('groups')
        is_superuser = data.get('is_superuser')
        is_staff = data.get('is_staff')
        serializer = ApproveRejectRequestSerializer(user_instance, data=data)
        if serializer.is_valid():
            serializer.save(is_role_requested="Accepted")
            auth_user_instance.groups.set(group)
            if is_staff == "True" and is_superuser == "True":
                auth_user_instance.is_superuser = True
                auth_user_instance.is_staff = True
            elif is_staff == "True":
                auth_user_instance.is_staff = True
                auth_user_instance.is_superuser = False
            elif is_superuser == "True":
                auth_user_instance.is_superuser = True
                auth_user_instance.is_staff = False
            else:
                auth_user_instance.is_superuser = False
                auth_user_instance.is_staff = False
            auth_user_instance.save()
            subject, from_email = 'Request For Role', settings.DEFAULT_FROM_EMAIL

            email_host_password = KeyDetail.objects.get(keyname="email_host_password").value
            send_mail(
                subject,
                'Your request to change the role has been accepted',
                from_email,
                [to_email],
                fail_silently=False,
                auth_password=email_host_password
            )
            return Response([{"message": "Request Approved Successfully", "status": True}], status=200)
        return Response([{"Data": serializer.errors, "message": "Error", "status": False}], status=400)


class RejectRoleRequest(APIView):
    """Viewset for rejected role request"""
    permissions_classes = [IsAuthenticated, ]

    def get_object(self, user):
        try:
            return UserDetail.objects.get(user=user)
        except User.DoesNotExist:
            raise Http404

    def put(self, request, user=None):
        data = request.data
        user_instance = self.get_object(user)
        auth_user_instance = User.objects.get(id=user)
        to_email = auth_user_instance.email
        serializer = RejectRequestSerializer(user_instance, data=data)
        if serializer.is_valid():
            serializer.save(is_role_requested="Rejected")

            email_host_password = KeyDetail.objects.get(keyname="email_host_password").value
            subject, from_email = 'Request For Role', settings.DEFAULT_FROM_EMAIL
            send_mail(
                subject,
                'Sorry! Your request to change the role has been declined',
                from_email,
                [to_email],
                fail_silently=False,
                auth_password=email_host_password
            )
            return Response([{"message": "Request Rejected Succesfully", "status": True}], status=200)
        return Response([{"Data": serializer.errors, "message": "Error", "status": False}], status=400)


def validate_oauth_token(token):
    """Validate the token code from a mobile client SSO login, then return the user's DRF token
    for use in authenticating future requests to this API.

    https://developers.google.com/identity/sign-in/android/backend-auth#using-a-google-api-client-library
    """
    try:
        idinfo = client.verify_id_token(token, settings.GOOGLE_OAUTH_CLIENT_ID)
        if idinfo['aud'] not in [settings.GOOGLE_OAUTH_CLIENT_ID]:
            return JsonResponse({'error': 'Unrecognized client.'}, status=status.HTTP_403_FORBIDDEN)
        if idinfo['iss'] not in ['accounts.google.com', 'https://accounts.google.com']:
            return JsonResponse({'error': 'Wrong issuer.'}, status=status.HTTP_403_FORBIDDEN)
        # have a good token; get API token now
        user = authenticate(**idinfo)
        if user:
            logger.debug('validated SSO token code for user: {email}'.format(email=user.email))
            token, created = Token.objects.get_or_create(user=user)
            return Response({'token': token.key, 'user': token.user_id})
        else:
            return JsonResponse({'error': 'This login is not valid in this application'},
                                status=status.HTTP_403_FORBIDDEN)
    except crypt.AppIdentityError:
        return JsonResponse({'error': 'Invalid token'}, status=status.HTTP_403_FORBIDDEN)


class DriverSsoAuthToken(APIView):
    parser_classes = (JSONParser,)
    permission_classes = (AllowAny,)

    def post(self, request, format=None):
        token = request.data.get('token')
        if token:
            return validate_oauth_token(token)
        else:
            return JsonResponse({'error': 'Token parameter is required'}, status=status.HTTP_400_BAD_REQUEST)


# login & generating user token
class DriverObtainAuthToken(ObtainAuthToken):
    permission_classes = []
    authentication_classes = []

    def post(self, request):
        serializer = self.serializer_class(data=request.data, context={'request': request})
        serializer.is_valid(raise_exception=True)
        user = serializer.validated_data['user']
        adv_user_obj = UserDetail.objects.get(user=user.id)
        token, created = Token.objects.get_or_create(user=user)
        group_list = []
        for group in token.user.groups.all():
            group_list.append(group.name)
            group_list.append(group.id)
        # Override method to include `user` in response
        return Response([{'token': token.key,
                          'username': token.user.username,
                          'group': group_list,
                          'user': token.user_id,
                          'first name': token.user.first_name,
                          'last name': token.user.last_name,
                          'email_address': token.user.email,
                          'active': token.user.is_active,
                          'staff_status': token.user.is_staff,
                          'superuser_status': token.user.is_superuser,
                          'is_analyst': adv_user_obj.is_analyst,
                          'is_tech_analyst': adv_user_obj.is_tech_analyst
                          }])


obtain_auth_token = DriverObtainAuthToken.as_view()
sso_auth_token = DriverSsoAuthToken.as_view()


# register new user
class UserRegistrationAPI(APIView):
    permission_classes = []
    authentication_classes = []

    def get(self, request):
        queryset = User.objects.filter(is_active=True)
        serializer = UserSerializer(queryset, many=True, context={'request': request})
        return Response([{"data": [serializer.data], "message": "success", "status": True}], status=200)

    @transaction.atomic
    def post(self, request):
        data = request.data
        serializer = UserSerializer(data=data, context={'request': request})
        if serializer.is_valid():
            password = make_password(self.request.data['password'])
            data = serializer.save(password=password)
            user_id = data.id
            hashed_password = hashlib.sha256(password.encode("utf-8")).hexdigest()[0:10]
            PasswordHistory.objects.create(user_id=data, hashed_password=hashed_password)
            user = data
            Token.objects.get_or_create(user=user)
            token = Token.objects.get(user=user)
            group_list = []
            for group in token.user.groups.all():
                group_list.append(group.name)
                group_list.append(group.id)
            token_dict = {'token': token.key}
            group_dict = {'groupdetail': group_list}
            to_email = serializer.validated_data['email']
            subject, from_email, to = 'DRIVER 2.0 Registration', settings.DEFAULT_FROM_EMAIL, to_email,
            html_content = (
                '<h2>Congratulations !!</h2><br><h5>You have successfully registered with DRIVER 2.0<h5>'.format(
                    to_email,
                    user_id))
            text_content = ' '
            if subject and from_email:
                try:
                    email_host_password = KeyDetail.objects.get(keyname="email_host_password").value
                    settings.EMAIL_HOST_PASSWORD = email_host_password
                    msg = EmailMultiAlternatives(subject, text_content, from_email, [to])
                    msg.attach_alternative(html_content, "text/html")
                    msg.send()
                except BadHeaderError:
                    return JsonResponse({'error': 'Invalid Credentials'}, status=status.HTTP_400_BAD_REQUEST)
            else:
                return JsonResponse({'error': 'Please Enter all fields correctly'}, status=status.HTTP_400_BAD_REQUEST)
            token_dict.update(serializer.data)
            group_dict.update(token_dict)
            return Response([group_dict], status=200)
        return Response([serializer.errors], status=400)


# get_by_id update and delete user
class UserRegistrationDetailAPI(APIView):
    permission_classes = []
    authentication_classes = []

    def get_object(self, id):
        try:
            return User.objects.get(id=id)
        except User.DoesNotExist:
            raise Http404

    def get(self, request, id=None):
        user_instance = self.get_object(id)
        serializer = UserSerializer(user_instance, context={'request': request})
        return Response([{"data": serializer.data, "message": "success", "status": True}], status=200)

    def put(self, request, id=None):
        data = request.data
        user_instance = self.get_object(id=id)
        serializer = UserSerializer(user_instance, data=data, context={'request': request})
        if serializer.is_valid():
            serializer.save()
            return Response([{"data": serializer.data, "message": "success", "status": True}], status=200)
        return Response([serializer.errors], status=400)

    def delete(self, request, id=None):
        user_instance = self.get_object(id=id)
        user = User.objects.get(id=id)
        user.is_active = False
        user.save()
        driver_user = UserDetail.objects.get(user=id)
        driver_user.is_active = False
        driver_user.save()
        return Response([{"message": "Details deleted successfully", "status": "true"}], status=200)


# get and post user details in UserDetail
class AdvUserRegisterAPI(APIView):
    """Viewset for save user details with additional information eg. City,Organisation,reg.. etc"""
    permission_classes = []

    # authentication_classes = []

    def get(self, request):
        get_data = request.query_params
        paginator = OptionalLimitOffsetPagination()
        if get_data['groups'] != '0' and get_data['user'] != 'blank':
            criterion1 = (Q(first_name__iexact=get_data['user'])
                          | Q(last_name__iexact=get_data['user'])
                          | Q(email=get_data['user']))
            criterion2 = (Q(groups__id=get_data['groups'])
                          & Q(is_active=True))
            user_obj = UserDetail.objects.filter(criterion1 & criterion2).order_by('-date_joined').distinct()
        elif get_data['groups'] != '0' and get_data['user'] == 'blank':
            user_obj = UserDetail.objects.filter(is_active=True, groups__id=get_data['groups']).order_by('-date_joined')
        elif get_data['user'] != 'blank' and get_data['groups'] == '0':
            user_obj = UserDetail.objects.filter(Q(first_name__iexact=get_data['user'])
                                                 | Q(last_name__iexact=get_data['user'])
                                                 | Q(email=get_data['user'], is_active=True)) \
                .order_by('-date_joined').distinct()

        else:
            user_obj = UserDetail.objects.filter(is_active=True, ).order_by('-date_joined')
        quesryset = paginator.paginate_queryset(user_obj, request)
        serializer = AdvUserSerializer(quesryset, many=True)
        return paginator.get_paginated_response([serializer.data])

    def post(self, request):
        data = request.data
        serializer = AdvUserSerializer(data=data)
        if serializer.is_valid():
            password = make_password(self.request.data['password'])
            serializer.save(password=password)
            return Response([serializer.data], status=200)
        return Response([serializer.errors], status=400)


# get_by_id, update and delete user details in UserDetail
class AdvUserRegisterDetailAPI(APIView):
    permission_classes = []
    authentication_classes = []

    def get_object(self, id):
        try:
            return UserDetail.objects.get(id=id)
        except UserDetail.DoesNotExist:
            raise Http404

    def get(self, request, id=None):
        user_detail_obj = self.get_object(id=id)
        serializer = AdvUserSerializer(user_detail_obj)
        return Response([serializer.data], status=200)

    @transaction.atomic
    def put(self, request, id=None):
        data = request.data
        user_detail_obj = self.get_object(id=id)
        serializer = AdvUserSerializer(user_detail_obj, data=data)
        old_password = user_detail_obj.password
        auth_user = User.objects.get(username=user_detail_obj.user)
        group = self.request.data['groups'][0]
        group_obj = Group.objects.get(name=group)
        group_list = [group_obj.id]
        if serializer.is_valid():
            if user_detail_obj.google_user == True:
                serializer.save()
                auth_user.is_active = self.request.data['is_active']
                auth_user.is_staff = self.request.data['is_staff']
                auth_user.is_superuser = self.request.data['is_superuser']
                auth_user.first_name = self.request.data['first_name']
                auth_user.last_name = self.request.data['last_name']
                auth_user.username = self.request.data['username']
                auth_user.email = self.request.data['email']
                return Response([{"data": serializer.data, "message": "success", "status": True}], status=200)
            elif self.request.data['password'] == "":
                self.request.data['password'] = old_password
                serializer.save(password=old_password)
                auth_user.password = old_password
                auth_user.is_active = self.request.data['is_active']
                auth_user.is_staff = self.request.data['is_staff']
                auth_user.is_superuser = self.request.data['is_superuser']
                auth_user.first_name = self.request.data['first_name']
                auth_user.last_name = self.request.data['last_name']
                auth_user.username = self.request.data['username']
                auth_user.email = self.request.data['email']
                auth_user.groups.set(group_list)
                auth_user.save()
                return Response([{"data": serializer.data, "message": "success", "status": True}], status=200)
            else:
                password = make_password(self.request.data['password'])
                serializer.save(password=password)

                auth_user.password = password

                hashed_password = hashlib.sha256(password.encode("utf-8")).hexdigest()[0:10]
                PasswordHistory.objects.create(user_id=auth_user, hashed_password=hashed_password)

                auth_user.is_active = self.request.data['is_active']
                auth_user.is_staff = self.request.data['is_staff']
                auth_user.is_superuser = self.request.data['is_superuser']
                auth_user.first_name = self.request.data['first_name']
                auth_user.last_name = self.request.data['last_name']
                auth_user.username = self.request.data['username']
                auth_user.email = self.request.data['email']
                auth_user.groups.set(group_list)
                auth_user.save()
                return Response([{"data": serializer.data, "message": "success", "status": True}], status=200)
        return Response([{"data": serializer.errors, "status": False}], status=400)


class UserDetails(APIView):
    permissions_classes = [IsAuthenticated, ]

    def get_object(self, id):
        try:
            return User.objects.get(id=id)
        except User.DoesNotExist:
            raise Http404

    def get(self, request, id=None):
        user_detail_obj = self.get_object(id=id)
        token = Token.objects.get(user=user_detail_obj)
        group_list = []
        for group in token.user.groups.all():
            group_list.append(group.id)
        group_dict = {'groupdetail': group_list}
        token_dict = {'token': token.key}
        serializer = DetailsSerializer(user_detail_obj)
        token_dict.update(serializer.data)
        group_dict.update(token_dict)
        return Response([{"data": [group_dict], "message": "success", "status": True}], status=200)


# TO get region-wise cities
class RegionCities(APIView):

    def get(self, request, **uuids):
        region_uuid_list = []
        query_list = []
        region_uuid_list.append(uuids)
        all_cities = City.objects.all()
        for region_uuids in region_uuid_list:
            for region_uuid in region_uuids["uuids"].split(','):
                queryset = all_cities.filter(region_id=region_uuid)
                query_list.extend(queryset)
        serializer = CitySerializer(query_list, many=True)
        return Response([{"data": serializer.data, "message": "success", "status": True}], status=200)


# TO get region-wise organization
class RegionOrganization(APIView):
    """Viewset to get region wise organisations"""

    def get(self, request, **uuids):
        region_uuid_list = []
        query_list = []
        region_uuid_list.append(uuids)
        all_organizations = Organization.objects.all()
        for region_uuids in region_uuid_list:
            for region_uuid in region_uuids["uuids"].split(','):
                queryset = all_organizations.filter(region_id=region_uuid)
                query_list.extend(queryset)
        serializer = CitySerializer(query_list, many=True)
        return Response([{"data": serializer.data, "message": "success", "status": True}], status=200)

class RoleDetails(APIView):
    """Viewset to get the details of requested role,useful for accept/reject role request"""

    def get_object(self, id):
        try:
            return SendRoleRequest.objects.filter(user=id)
        except User.DoesNotExist:
            raise Http404

    def get(self, request, id=None):
        city_list = []
        org_list = []
        requested_city_list = []
        requested_org_list = []
        requested_group_list = []
        current_region_list = []
        user = UserDetail.objects.filter(user=id).last()
        requested_user = SendRoleRequest.objects.filter(user=id).last()
        for city in user.city.all():
            city_list.append(city.uuid)
        for org in user.org.all():
            org_list.append(org.name)
        for city in requested_user.city.all():
            requested_city_list.append(city.uuid)
        for org in requested_user.org.all():
            requested_org_list.append(org.name)
        for reg in user.reg.all():
            current_region_list.append(reg.uuid)
        requested_group_list.append(requested_user.group.id)
        group_obj = Groupdetail.objects.get(group=requested_user.group.id)
        user_dict = {'name': user.first_name + ' ' + user.last_name,
                     'username': user.username,
                     'current_city': city_list,
                     'current_organization': org_list,
                     'requested_city': requested_city_list,
                     'requested_org': requested_org_list,
                     'current_region': current_region_list,
                     'requested_group': requested_user.group.name,
                     'group_id': requested_group_list,
                     'admin_status': group_obj.is_admin
                     }
        serializer = RequestSerializer(requested_user)
        user_dict.update(serializer.data)
        return Response([{"data": user_dict, "message": "success", "status": True}], status=200)


class RegionNames(APIView):

    def get(self, request, **uuids):
        region_uuid_list = []
        query_list = []
        region_uuid_list.append(uuids)
        all_regions = BoundaryPolygon.objects.all()
        for region_uuids in region_uuid_list:
            for region_uuid in region_uuids["uuids"].split(','):
                queryset = all_regions.filter(uuid=region_uuid)
                query_list.extend(queryset)
        serializer = BoundaryPolygonSerializer(query_list, many=True)
        return Response([{"data": serializer.data, "message": "success", "status": True}], status=200)


class CheckRoleStatus(APIView):
    """Viewset to check the status of user's role_request before requesting for a role"""
    permissions_classes = [IsAuthenticated, ]

    def get_object(self, id):
        try:
            return UserDetail.objects.get(user=id)
        except UserDetail.DoesNotExist:
            raise Http404

    def get(self, request, id=None):
        user_detail_obj = self.get_object(id=id)
        serializer = RejectRequestSerializer(user_detail_obj)
        return Response([{"data": [serializer.data], "message": "success", "status": True}], status=200)


class ReturnUserToken(APIView):
    def get_object(self, id):
        try:
            return Token.objects.get(user=id)
        except Token.DoesNotExist:
            raise Http404

    def get(self, request, id=None):
        user_obj = self.get_object(id=id)
        user = User.objects.get(id=id)
        group_list = []
        for group in user.groups.all():
            group_list.append(group.id)
            group_list.append(group.name)
        group_dict = {'groupdetail': group_list}
        if user.is_active == True:
            serializer = TokenSerializer(user_obj)
            group_dict.update(serializer.data)
            return Response([{"data": group_dict, "message": "Success", "status": True}])
        else:
            return Response([{"data": "Details for the user not available", "token": "False", "message": "Success",
                              "status": True}])


class CityRegion(APIView):
    def get(self, request, id):
        queryset = BoundaryPolygon.objects.filter(boundary_id=id)
        serializer = BoundaryPolygonSerializer(queryset, many=True)
        return Response({"data": serializer.data, "status": "true"}, status=200)


class RecordCopy(APIView):
    def get(self, request):
        queryset = DriverRecordCopy.objects.all()
        serializer = DriverRecordCopySerializer(queryset, many=True)
        return Response({"data": serializer.data, "status": "true"}, status=200)

    def post(self, request, format=None):
        serializer = DriverRecordCopySerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response({"message": serializer.data, "status": "true"}, status=200)
        return Response({"message": serializer.errors, "status": "false"}, status=400)


class RecordCopyDetails(APIView):
    def get(self, request, uuid):
        queryset = DriverRecordCopy.objects.filter(record=uuid)
        original_record_obj = DriverRecord.objects.get(uuid=uuid)
        merged_details = {'merged_and_updated': original_record_obj.merged_and_updated,
                          'merged_uuid': original_record_obj.merged_uuid}
        serializer = DriverRecordCopySerializer(queryset, many=True, context={'request': request})
        # merged_details.update(serializer.data[0])
        # merged_details.update({'data': serializer.data})
        return Response(serializer.data)


class WeatherInfoDetails(APIView):
    permissions_classes = [IsAuthenticated, ]

    def get(self, request):
        queryset = WeatherInfo.objects.all().exclude(is_deleted=True)
        serializer = WeatherInfoSerializer(queryset, many=True)
        return Response({"data": serializer.data, "status": True})

    def post(self, request):
        serializer = WeatherInfoSerializer(data=request.data)
        provider_name = request.data.get('provider_name')
        if serializer.is_valid():
            if not WeatherInfo.objects.filter(provider_name=provider_name).exclude(is_deleted=True).exists():
                for other_obj in WeatherInfo.objects.all():
                    other_obj.is_active = False
                    other_obj.save()
                serializer.save()
                return Response({"message": "Details added successfully", "status": True}, status=200)
            else:
                return Response({"message": "Details already exist", "status": True}, status=200)
        return Response({"message": serializer.errors, "status": False}, status=400)


class WeatherInfoDetailById(APIView):
    permissions_classes = [IsAuthenticated, ]

    def get_object(self, id):
        try:
            return WeatherInfo.objects.get(id=id)
        except UserDetail.DoesNotExist:
            raise Http404

    def get(self, request, id=None):
        weather_api_obj = self.get_object(id=id)
        serializer = WeatherInfoSerializer(weather_api_obj)
        return Response([serializer.data], status=200)

    def put(self, request, id=None):
        data = request.data
        weather_api_obj = self.get_object(id=id)
        serializer = WeatherInfoSerializer(weather_api_obj, data=data)
        provider_name = request.data.get('provider_name')

        if serializer.is_valid():
            if not WeatherInfo.objects.filter(provider_name=provider_name).exclude(
                    Q(id=id) | Q(is_deleted=True)).exists():
                serializer.save()
                for other_obj in WeatherInfo.objects.all().exclude(id=id):
                    other_obj.is_active = False
                    other_obj.save()
                return Response([{"message": "Details Updated Successfully", "status": True}],
                                status=200)
            else:
                return Response([{"message": "Details already exist", "status": True}],
                                status=200)
        return Response([{"data": serializer.errors, "status": False}], status=400)

    def delete(self, request, id=None):
        instance = self.get_object(id)
        instance.is_deleted = True
        instance.save()
        return Response([{"message": "Details deleted successfully", "status": True}], status=200)


class RegisterGoogleUser(APIView):
    """Viewset for login-with-google"""
    permission_classes = []
    authentication_classes = []

    @transaction.atomic
    def post(self, request):
        serializer = GoogleUserSerializer(data=request.data, context={'request': request})
        email = request.data.get('email')
        name = request.data.get('name')
        # groups = request.data.get("groups")
        name = name.split()
        fname = name[0]
        lname = name[1]
        try:
            user_obj = User.objects.get(email=email)
            adv_user_obj = UserDetail.objects.get(user=user_obj.id)
            token, created = Token.objects.get_or_create(user=user_obj.id)
            group_list = []
            for group in token.user.groups.all():
                group_list.append(group.name)
                group_list.append(group.id)
            return Response([{'token': token.key,
                              'username': token.user.username,
                              'groupdetail': group_list,
                              'user': token.user_id,
                              'first name': token.user.first_name,
                              'last name': token.user.last_name,
                              'email': token.user.email,
                              'active': token.user.is_active,
                              'staff_status': token.user.is_staff,
                              'superuser_status': token.user.is_superuser,
                              'is_analyst': adv_user_obj.is_analyst,
                              'is_tech_analyst': adv_user_obj.is_tech_analyst,
                              'google_user': adv_user_obj.google_user
                              }])
        except User.DoesNotExist:
            user_obj = None
        if user_obj == None:
            group_obj = Group.objects.get(name="Public")
            if serializer.is_valid():
                data = serializer.save(first_name=fname, last_name=lname, username=email)
                user = data
                user.groups.set([group_obj.id])
                user.save()
                user_detail_obj = UserDetail(user=user, first_name=fname, last_name=lname,
                                             username=email, email=email, google_user=True)
                user_detail_obj.save()
                user_detail_obj.groups.set([group_obj.id])
                user_detail_obj.save()
                Token.objects.get_or_create(user=user)
                token = Token.objects.get(user=user)
                group_list = []
                group_id_list = []
                for group in token.user.groups.all():
                    group_list.append(group.name)
                    group_list.append(group.id)
                    group_id_list.append(group.id)
                user_detail_obj.groups.set(group_id_list)
                token_dict = {'token': token.key, 'groups': ["Public"]}
                group_dict = {'groupdetail': group_list}
                user_id = data.id
                to_email = serializer.validated_data['email']
                subject, from_email, to = 'DRIVER 2.0 Registration', settings.DEFAULT_FROM_EMAIL, to_email,
                html_content = (
                    '<h2>Congratulations !!</h2><br><h5>You have successfully registered with DRIVER 2.0<h5>'.format(
                        to_email,
                        user_id))
                text_content = ' '
                if subject and from_email:
                    try:
                        email_host_password = KeyDetail.objects.get(keyname="email_host_password").value
                        settings.EMAIL_HOST_PASSWORD = email_host_password
                        msg = EmailMultiAlternatives(subject, text_content, from_email, [to])
                        msg.attach_alternative(html_content, "text/html")
                        msg.send()
                    except BadHeaderError:
                        return JsonResponse({'error': 'Invalid Credentials'}, status=status.HTTP_400_BAD_REQUEST)
                else:
                    return JsonResponse({'error': 'Please Enter all fields correctly'},
                                        status=status.HTTP_400_BAD_REQUEST)
                token_dict.update(serializer.data)
                user_detail_dict = {'is_analyst': user_detail_obj.is_analyst,
                                    'username': user_detail_obj.username,
                                    'is_tech_analyst': user_detail_obj.is_tech_analyst,
                                    'google_user': user_detail_obj.google_user,
                                    'user': user_detail_obj.user.id}
                group_dict.update(token_dict)
                group_dict.update(user_detail_dict)
                return Response([group_dict], status=200)
            return Response([serializer.errors], status=400)


class MergedAndUpdatedRecords(APIView):
    """Viewset to get details of records which merged/updated while resolving the duplicates"""

    def get(self, request, **uuids):
        record_uuid_list = []
        query_list = []
        record_uuid_list.append(uuids)
        all_records = DriverRecord.objects.all()
        for record_uuids in record_uuid_list:
            for region_uuid in record_uuids["uuids"].split(','):
                queryset = all_records.filter(record_ptr_id=region_uuid)
                query_list.extend(queryset)
        serializer = BaseDriverRecordSerializer(query_list, many=True)
        return Response([{"data": serializer.data, "message": "success", "status": True}], status=200)


class FindExisting(APIView):
    """Viewset to get existing records with the same details if exist"""

    def post(self, request):
        threshold_obj = DuplicateDistanceConfig.objects.all().order_by('pk').first()
        distance = threshold_obj.dedupe_distance_threshold
        distance_allowance = distance
        time_allowance = datetime.timedelta(hours=settings.DEDUPE_TIME_RANGE_HOURS)
        oc_from = request.data.get("occurred_from")
        coords = request.data.get("geom")['coordinates']
        is_update = request.data.get("is_update")
        rec_uuid = request.data.get('rec_uuid')
        longitude = coords[0]
        latitude = coords[1]
        with connections['default'].cursor() as cursor:
            cursor.execute("SELECT ST_SetSRID( ST_Point({long}, {lat}), 4326)".format(long=longitude, lat=latitude))
            for result in cursor.fetchall():
                geometry_val = result[0]
            oc_date = strf_date.strptime(oc_from, "%Y-%m-%dT%H:%M:%S%z")
            if is_update == "True":
                filtered_queryset = DriverRecord.objects.filter(
                    geom__dwithin=(geometry_val, distance_allowance),
                    occurred_from__range=(
                        oc_date - time_allowance,
                        oc_date + time_allowance
                    )).exclude(Q(archived=True) |
                               Q(uuid=rec_uuid))
            else:
                filtered_queryset = DriverRecord.objects.filter(
                    geom__dwithin=(geometry_val, distance_allowance),
                    occurred_from__range=(
                        oc_date - time_allowance,
                        oc_date + time_allowance
                    )).exclude(archived=True)

            serializer = BaseDriverRecordSerializer(filtered_queryset, many=True)
        return Response(serializer.data)


class GetJsonSchemaKey(APIView):

    def post(self, request):

        rec_type_obj = RecordType.objects.filter(label="Incident").last()
        record_type_id = rec_type_obj.uuid
        base_url = str(settings.HOST_URL) + "/data-api/latestrecordschema/"
        token = request.META.get('HTTP_AUTHORIZATION')
        response = requests.post(base_url,
                                 data={"record_type_id": record_type_id},
                                 headers={"Authorization": token}
                                 )
        request_data = request.data.get('property_key')
        for key in response.json()["result"][0]["schema"]["definitions"]:
            for i in response.json()["result"][0]["schema"]["definitions"][key]["properties"].keys():
                if request_data == i:
                    containment = response.json()["result"][0]["schema"]["definitions"][key]['multiple']
                    if containment == True:
                        containment = "containment_multiple"
                    else:
                        containment = "containment"
                    value_to_return = key
        return Response({"value": value_to_return, "containment": containment})


class CountryDetails(viewsets.ModelViewSet):
    serializer_class = CountryInfoSerializer
    pagination_class = None
    filter_class = CountryInfoFilter

    def get_queryset(self):
        return CountryInfo.objects.all()

    def perform_create(self, serializer):
        if CountryInfo.objects.count() >= 1:
            country_obj = CountryInfo.objects.all()
            for country in country_obj:
                country.archived = False
                country.save()
            serializer.save()
        else:
            serializer.save()

    def perform_update(self, serializer):
        serializer.save()

    def perform_destroy(self, instance):
        instance.delete()


class LanguageDetailsViewSet(viewsets.ModelViewSet):
    serializer_class = LanguageDetailSerializer
    pagination_class = None
    filterset_class = LanguageDetailFilter
    permission_classes = []

    
    def get_queryset(self):
        return LanguageDetail.objects.all()

    def perform_create(self, serializer):
        if not LanguageDetail.objects.filter(language_code=self.request.data['language_code'], upload_for=self.request.
                data['upload_for']).exclude(archive=True).exists():
            data = {}
            host = settings.API_HOST
            filename = serializer.validated_data.get('language_code')
            base64_message = self.request.data['csv_f']
            upload_for = serializer.validated_data.get('upload_for')
            user_panel_default = serializer.validated_data.get('default_for_user_panel')
            admin_panel_default = serializer.validated_data.get('default_for_admin_panel')
            base64_message = base64_message.split("base64")[1]
            if upload_for == 'user_panel':
                csv_file_save_path = str(settings.STATIC_ROOT) + "media/multi-language/user-panel/language-csv"
                json_file_save_path = str(settings.STATIC_ROOT) + "media/multi-language/user-panel/language-json"
                if not os.path.exists(csv_file_save_path):
                    os.makedirs(csv_file_save_path)
                base64_img_bytes = base64_message.encode('utf-8')
                csv_file = os.path.join(csv_file_save_path, filename + ".csv")
                with open(csv_file, 'wb') as csv_data_file:
                    decoded_csv_data = base64.decodebytes(base64_img_bytes)
                    csv_data_file.write(decoded_csv_data)
                with open(csv_file) as f_c:
                    reader = csv.DictReader(f_c)
                    for r in reader:
                        data[r["key"]] = r["value"]
                if not os.path.exists(json_file_save_path):
                    os.makedirs(json_file_save_path)
                json_file = os.path.join(json_file_save_path, filename + ".json")
                with open(json_file, 'w') as json_data_file:
                    json_data_file.write(json.dumps(data, indent=4))
                if user_panel_default == True:
                    for obj in LanguageDetail.objects.all():
                        obj.default_for_user_panel = False
                        obj.save()
                # csv_file_path = str(settings.API_HOST) + ":" + settings.APACHE_PORT + csv_file.split('media')[1]
                csv_file_path = str(settings.HOST_URL) + '/download' + csv_file.split('media')[1]
                serializer.save(
                    csv_file=csv_file_path,
                    json_file=json_file
                )
            elif upload_for == 'admin_panel':
                "/ var / www / static / media"
                csv_file_save_path = str(settings.STATIC_ROOT) + "media/multi-language/ashlar-editor/language-csv"
                json_file_save_path = str(settings.STATIC_ROOT) + "media/multi-language/ashlar-editor/language-json"
                if not os.path.exists(csv_file_save_path):
                    os.makedirs(csv_file_save_path)
                base64_img_bytes = base64_message.encode('utf-8')
                csv_file = os.path.join(csv_file_save_path, filename + ".csv")
                with open(csv_file, 'wb') as csv_data_file:
                    decoded_csv_data = base64.decodebytes(base64_img_bytes)
                    csv_data_file.write(decoded_csv_data)
                with open(csv_file) as f_c:
                    reader = csv.DictReader(f_c)
                    for r in reader:
                        data[r["key"]] = r["value"]
                if not os.path.exists(json_file_save_path):
                    os.makedirs(json_file_save_path)
                json_file = os.path.join(json_file_save_path, filename + ".json")
                with open(json_file, 'w') as json_data_file:
                    json_data_file.write(json.dumps(data, indent=4))
                if admin_panel_default == True:
                    for obj in LanguageDetail.objects.all():
                        obj.default_for_admin_panel = False
                        obj.save()
                # csv_file_path = str(settings.API_HOST) + ":" + settings.APACHE_PORT + csv_file.split('media')[1]
                csv_file_path = str(settings.HOST_URL) + '/download' + csv_file.split('media')[1]
                serializer.save(
                    csv_file=csv_file_path,
                    json_file=json_file
                )
        else:
            raise serializers.ValidationError("Details already exist")

    def perform_update(self, serializer):
        if not LanguageDetail.objects.filter(language_code=self.request.data['language_code'], upload_for=self.request.
                data['upload_for']).exclude(Q(id=self.request.data['id'])
                                            | Q(archive=True)).exists():
            data = {}
            host = settings.API_HOST
            filename = serializer.validated_data.get('language_code')
            base64_message = self.request.data['csv_f']
            upload_for = serializer.validated_data.get('upload_for')
            user_panel_default = serializer.validated_data.get('default_for_user_panel')
            admin_panel_default = serializer.validated_data.get('default_for_admin_panel')
            base64_message = base64_message.split("base64")[1]
            if upload_for == 'user_panel':
                csv_file_save_path = str(settings.STATIC_ROOT) + "media/multi-language/user-panel/language-csv"
                json_file_save_path = str(settings.STATIC_ROOT) + "media/multi-language/user-panel/language-json"
                if not os.path.exists(csv_file_save_path):
                    os.makedirs(csv_file_save_path)
                base64_img_bytes = base64_message.encode('utf-8')
                csv_file = os.path.join(csv_file_save_path, filename + ".csv")
                with open(csv_file, 'wb') as csv_data_file:
                    decoded_csv_data = base64.decodebytes(base64_img_bytes)
                    csv_data_file.write(decoded_csv_data)
                with open(csv_file) as f_c:
                    reader = csv.DictReader(f_c)
                    for r in reader:
                        data[r["key"]] = r["value"]
                if not os.path.exists(json_file_save_path):
                    os.makedirs(json_file_save_path)
                json_file = os.path.join(json_file_save_path, filename + ".json")
                with open(json_file, 'w') as json_data_file:
                    json_data_file.write(json.dumps(data, indent=4))
                if user_panel_default == True:
                    for obj in LanguageDetail.objects.all():
                        obj.default_for_user_panel = False
                        obj.save()
                # csv_file_path = "http://" + host + csv_file.split('media')[1]
                # csv_file_path = str(settings.API_HOST) + ":" + settings.APACHE_PORT + csv_file.split('media')[1]
                csv_file_path = str(settings.HOST_URL) + '/download' + csv_file.split('media')[1]
                serializer.save(
                    csv_file=csv_file_path,
                    json_file=json_file
                )
            elif upload_for == 'admin_panel':
                csv_file_save_path = str(settings.STATIC_ROOT) + "media/multi-language/ashlar-editor/language-csv"
                json_file_save_path = str(settings.STATIC_ROOT) + "media/multi-language/ashlar-editor/language-json"
                if not os.path.exists(csv_file_save_path):
                    os.makedirs(csv_file_save_path)
                base64_img_bytes = base64_message.encode('utf-8')
                csv_file = os.path.join(csv_file_save_path, filename + ".csv")
                with open(csv_file, 'wb') as csv_data_file:
                    decoded_csv_data = base64.decodebytes(base64_img_bytes)
                    csv_data_file.write(decoded_csv_data)
                with open(csv_file) as f_c:
                    reader = csv.DictReader(f_c)
                    for r in reader:
                        data[r["key"]] = r["value"]
                if not os.path.exists(json_file_save_path):
                    os.makedirs(json_file_save_path)
                json_file = os.path.join(json_file_save_path, filename + ".json")
                with open(json_file, 'w') as json_data_file:
                    json_data_file.write(json.dumps(data, indent=4))
                if admin_panel_default == True:
                    for obj in LanguageDetail.objects.all():
                        obj.default_for_admin_panel = False
                        obj.save()
                # csv_file_path = "http://" + host + csv_file.split('media')[1]
                # scheme = request.scheme,host = request.get_host(),prefix = settings.CELERY_DOWNLOAD_PREFIX,download = 'download/',file = job_result.get()
                csv_file_path = str(settings.HOST_URL) + '/download' + csv_file.split('media')[1]
                serializer.save(
                    csv_file=csv_file_path,
                    json_file=json_file
                )
        else:
            raise serializers.ValidationError("Details already exist")

    def perform_destroy(self, instance):
        lang_obj = LanguageDetail.objects.get(id=instance.id)
        lang_obj.archive = True
        lang_obj.save()


@api_view(['GET'])
@authentication_classes([])
@permission_classes([])
def returntranslatedresponsecontent(self, lang_code, upload_for):
    try:
        lang_detail_obj = LanguageDetail.objects.filter(language_code=lang_code, upload_for=upload_for).last()
        json_file = open(lang_detail_obj.json_file)
        f = json_file.read()
        response = json.loads(f)
        return Response(response)
    except:
        return Response({"NA": "NA"})


@api_view(['GET'])
@authentication_classes([])
@permission_classes([])
def check_required_data(self):
    lang_obj = LanguageDetail.objects.all().count()
    schema_obj = RecordSchema.objects.all().count()
    geography_obj = Boundary.objects.all().count()
    country_obj = CountryInfo.objects.all().count()

    if (country_obj and lang_obj and schema_obj and geography_obj) < 1:
        return Response({"message": "Please check if the following are added: Language Translation, Schema, Geography and Country Details"})

    elif lang_obj == 1:
        return Response({"message": "Please make sure language for Ashlar Editor and User Panel is added"})

    else:
        return Response({"message": "True"})


@api_view(['GET'])
@authentication_classes([TokenAuthentication])
@permission_classes([IsAuthenticated])
def intervention_type_detail(self, uuid):
    try:
        response_dict = {}
        schema_obj = RecordSchema.objects.filter(record_type_id=uuid).last()
        enum_list = [enum for enum in schema_obj.schema["definitions"]["driverInterventionDetails"]["properties"]
        ["Type"]["enum"]]
        for enum in enum_list:
            try:
                irap_obj = IrapDetail.objects.get(irap_treatment_id=enum)
                response_dict[enum] = irap_obj.irap_treatment_name
            except:
                response_dict[enum] = enum
        return Response(response_dict)
    except Exception as e:
        raise Exception(e)



class Test(APIView):
    def get_object(self, pk):
        try:
            return User.objects.get(pk=pk)
        except User.DoesNotExist:
            raise Http404

    def put(self, request, pk, format=None):

        getprivatekey = self.get_object(pk)
        serializer = User(getprivatekey, data=request.data, context={'request': request})
        password = make_password(self.request.data['password'])

        #   serializer.save(password=password,user_permissions=[28])
        if serializer.is_valid():
            serializer.save(password=password, user_permissions=user_permissions)
            return Response(serializer.data, status=status.HTTP_201_CREATED)

        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


# RESET Password feature
@api_view(['POST'])
@authentication_classes([])
@permission_classes([])
def send_update_password_link(request):
    """
    Use: API to send link to the registered email id of the user for update password.
    """
    try:
        username = request.data["username"]
        password_reset_link = request.data["password_reset_link"]
    except KeyError:
        return Response(data={"message": "jsonkey error"}, status=status.HTTP_400_BAD_REQUEST)

    try:
        user_obj = User.objects.get(username=username)
        user_id = user_obj.id
        unique_string = f"{user_id}{uuid.uuid4()}"
        hash_key = hashlib.sha256(unique_string.encode()).hexdigest()
        user_detail_obj = UserDetail.objects.get(user_id=user_id)
        user_detail_obj.password_update_hash_key = hash_key
        user_detail_obj.password_update_hash_created_at = timezone.now()
        user_detail_obj.save()
    except User.DoesNotExist:
        return Response(data={"message": "Invalid Credentials"}, status=status.HTTP_400_BAD_REQUEST)

    try:
        email_host_password = KeyDetail.objects.get(keyname="email_host_password").value
        settings.EMAIL_HOST_PASSWORD = email_host_password

        subject = "DRIVER-Samoa Password Reset Instruction."
        subject, from_email, to = subject, "ganesh.ghuge@aventior.com", user_obj.email
        text_content = 'This is an important message.'
        html_content = f'<p>Hi <strong>{user_obj.username}</strong>,</p>' \
                       f'<p>As you have requested for reset password instructions, here they are, please follow the URL:' \
                       f'</p><a href={password_reset_link +"/"+ hash_key}>Reset Password</a>' \
                       f'<p>Alternatively, open the following url in your browser</p>' \
                       f'<p>{password_reset_link + "/" + hash_key}</p> <p style="color:red;"> <b> ** The link is valid only for 15 minutes. </b></p>'
        msg = EmailMultiAlternatives(subject, text_content, from_email, [to])
        msg.attach_alternative(html_content, "text/html")
        msg.send()
    except:
        return Response(data={"message": "SMTP Error"}, status=status.HTTP_400_BAD_REQUEST)

    return Response(data={"message": "Password Reset link has been sent to your registered email"}, status=status.HTTP_200_OK)


@api_view(["GET"])
@authentication_classes([])
@permission_classes([])
def validate_reset_password_url(request):
    """
    Use:- To validate the reset password URL.
    """
    try:
        hash_key = request.query_params.get('hash_key')
        user_detail_obj = UserDetail.objects.get(password_update_hash_key=hash_key)
        created_time = user_detail_obj.password_update_hash_created_at
        valid = timezone.now() <= created_time + timedelta(minutes=15)
        if not valid:
            return Response(data={"message": "The link is expired. Please reset password again."}, status=status.HTTP_200_OK)
    except UserDetail.DoesNotExist:
        return Response(data={"message": "The link is either not valid or expired. Please reset password again."}, status=status.HTTP_404_NOT_FOUND)

    return Response(status=status.HTTP_200_OK)


@api_view(['POST'])
@authentication_classes([])
@permission_classes([])
def update_password(request):
    """
    Use: API call to update user password.
    request body:
    {
            "hash_key": "",
            "password": ""
    }
    """
    try:
        hash_key = request.data["hash_key"]
        password = request.data["password"]
    except KeyError:
        return Response(data={"message": "Json key error"}, status=status.HTTP_400_BAD_REQUEST)

    try:
        user_id = UserDetail.objects.get(password_update_hash_key=hash_key).user_id
        user_obj = User.objects.get(id=user_id)
    except User.DoesNotExist:
        return Response(data={"message": "Invalid Credentials"}, status=status.HTTP_400_BAD_REQUEST)

    new_hashed_password = hashlib.sha256(password.encode("utf-8")).hexdigest()[0:10]

    previous_passwords = PasswordHistory.objects.filter(user_id=user_obj)
    for prev_password in previous_passwords:
        if prev_password.hashed_password == new_hashed_password:
            return Response(data={"message": "You cannot reuse your recent passwords."}, status=status.HTTP_400_BAD_REQUEST)

    PasswordHistory.objects.create(user_id=user_obj, hashed_password=new_hashed_password)

    if previous_passwords.count() >= 3:
        obj = previous_passwords.order_by('-created_on').last()
        obj.delete()

    user_obj.set_password(password)
    user_obj.save()
    user_detail_obj = UserDetail.objects.get(user_id=user_obj.id)
    user_detail_obj.password_updated_date = datetime.date.today()
    user_detail_obj.save()

    return Response(data={"message": "Password Successfully Updated"}, status=status.HTTP_200_OK)
