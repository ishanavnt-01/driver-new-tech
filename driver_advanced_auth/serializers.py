import six
from django.contrib.auth.models import User, Permission, Group
from rest_framework.authtoken.models import Token
from rest_framework.validators import UniqueValidator
from rest_framework import serializers
from .models import UserDetail, Organization, City, Groupdetail, SendRoleRequest, CountryInfo, LanguageDetail
from django.contrib.contenttypes.models import ContentType


class GroupStringRelatedField(serializers.StringRelatedField):
    """
    StringRelatedField in DRF is read-only.
    Make it writeable for user groups, based on group name.
    """

    def to_internal_value(self, data):
        """
        Implement this field method so groups field may be writeable
        """
        if not isinstance(data, six.text_type):
            msg = 'Incorrect type. Expected a string, but got %s'
            raise serializers.ValidationError(msg % type(data).__name__)
        return Group.objects.get(name=data)


class CitySerializer(serializers.ModelSerializer):
    class Meta:
        model = City
        fields = ('id', 'name', 'country_id', 'region_id')


class OrganizationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Organization
        fields = ('id', 'country', 'region', 'name')


class AssociateGroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = Groupdetail
        fields = ('id', 'name', 'group', 'description', 'is_admin')

    name = serializers.CharField(validators=[UniqueValidator(queryset=Groupdetail.objects.all())])


class ContentTypeSerializer(serializers.ModelSerializer):
    class Meta:
        model = ContentType
        fields = ('id', 'app_label', 'model')


class PermissionSerializers(serializers.ModelSerializer):
    class Meta:
        model = Permission
        fields = ('id', 'name', 'content_type', 'codename')


class GroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = Group
        fields = ('id', 'name', 'permissions',)
        read_only_fields = ('id',)
    # name = serializers.CharField(validators=[UniqueValidator(queryset=Group.objects.all())])


# for getting goups with permissions
class GroupSerializerNew(serializers.ModelSerializer):
    permissions = PermissionSerializers(many=True)

    class Meta:
        model = Group
        fields = ('id', 'name', 'permissions',)
        read_only_fields = ('id',)


class RequestSerializer(serializers.ModelSerializer):
    class Meta:
        model = SendRoleRequest
        fields = ('reg', 'country', 'user', 'group', 'current_group', 'city', 'org')


class AdvAuthSerializer(serializers.ModelSerializer):
    class Meta:
        model = UserDetail
        field = '__all__'


class UserSerializer(serializers.ModelSerializer):
    # display groups by name
    groups = GroupStringRelatedField(many=True)

    class Meta:
        model = User
        fields = ('id', 'first_name', 'last_name', 'username', 'groups', 'email', 'date_joined', 'is_staff',
                  'is_superuser')

    email = serializers.EmailField(required=True, validators=[UniqueValidator(queryset=User.objects.all())])
    username = serializers.CharField(validators=[UniqueValidator(queryset=User.objects.all())])


class AdvUserSerializer(serializers.ModelSerializer):
    groups = GroupStringRelatedField(many=True)

    class Meta:
        model = UserDetail
        fields = ('id', 'first_name', 'last_name', 'email', 'username', 'geography', 'reg',
                  'city', 'org', 'groups', 'is_active', 'date_joined', 'updated_on', 'user', 'is_role_requested',
                  'mobile_no', 'is_staff', 'is_superuser', 'is_analyst', 'is_tech_analyst', 'google_user')
        # read_only_fields = ('user',)

    email = serializers.EmailField(required=True, validators=[UniqueValidator(queryset=UserDetail.objects.all())])
    username = serializers.CharField(validators=[UniqueValidator(queryset=UserDetail.objects.all())])


class DetailsSerializer(serializers.ModelSerializer):
    groups = GroupStringRelatedField(many=True)

    class Meta:
        model = User
        fields = ('id', 'first_name', 'last_name', 'username', 'email', 'groups', 'is_staff', 'is_superuser',
                  'is_active', 'date_joined')

class ApproveRejectRequestSerializer(serializers.ModelSerializer):
    class Meta:
        model = UserDetail
        fields = (
            'groups', 'city', 'reg', 'org', 'user', 'is_role_requested', 'is_tech_analyst', 'is_analyst',
            'is_superuser', 'is_staff')


class RejectRequestSerializer(serializers.ModelSerializer):
    # groups = GroupStringRelatedField(many=True)
    class Meta:
        model = UserDetail
        fields = ('is_role_requested',)


class TokenSerializer(serializers.ModelSerializer):
    class Meta:
        model = Token
        fields = ('key',)


class GoogleUserSerializer(serializers.ModelSerializer):
    # groups = GroupStringRelatedField(many=True)

    class Meta:
        model = User
        fields = ('id', 'first_name', 'last_name', 'email', 'date_joined',
                  'is_staff', 'is_superuser')


class CountryInfoSerializer(serializers.ModelSerializer):
    class Meta:
        model = CountryInfo
        fields = '__all__'


class LanguageDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = LanguageDetail
        fields = '__all__'
