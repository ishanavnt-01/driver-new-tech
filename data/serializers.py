import re
from rest_framework.serializers import (
    CharField,
    SerializerMethodField,
    ValidationError,
)
from grout import serializers
from grout import serializer_fields
from .models import CrashDiagramOrientation, DriverRecord, DriverRecordCopy, RecordAuditLogEntry, RecordDuplicate, \
    RecordCostConfig, DuplicateDistanceConfig, BulkUploadDetail, WeatherDataList, KeyDetail
from django.conf import settings
from rest_framework.serializers import ModelSerializer
from rest_framework_gis.serializers import GeoFeatureModelSerializer, GeoModelSerializer
from grout.models import Boundary, BoundaryPolygon, Record, RecordType, RecordSchema
from grout.serializer_fields import JsonBField, JsonSchemaField, GeomBBoxField
from data.models import WeatherInfo


class BaseDriverRecordSerializer(serializers.RecordSerializer):
    class Meta:
        model = DriverRecord
        fields = '__all__'
        read_only_fields = ('uuid',)

    # def validate_occurred_from(self, value):
    #     """ Require that record occurred_from be in the past. """
    #     if value > datetime.datetime.now(pytz.timezone(settings.TIME_ZONE)):
    #         raise ValidationError('Occurred date must not be in the future.')
    #     return value


class RecordSerializer(GeoModelSerializer):
    data = JsonBField()

    class Meta:
        model = Record
        fields = '__all__'
        read_only_fields = ('uuid',)


class RecordTypeSerializer(ModelSerializer):
    # current_schema = serializers.SerializerMethodField()

    class Meta:
        model = RecordType
        fields = '__all__'

    def get_current_schema(self, obj):
        current_schema = obj.get_current_schema()
        uuid = None
        if current_schema:
            uuid = str(current_schema.uuid)
        return uuid


class RecordSchemaSerializer(ModelSerializer):
    schema = JsonSchemaField()

    def create(self, validated_data):
        """Creates new schema or creates new version and updates next_version of previous"""
        if validated_data['version'] > 1:  # Viewset's get_serializer() will always add 'version'
            with transaction.atomic():
                current = RecordSchema.objects.get(record_type=validated_data['record_type'],
                                                   next_version=None)
                new = RecordSchema.objects.create(**validated_data)
                current.next_version = new
                current.save()
        elif validated_data['version'] == 1:  # New record_type
            new = RecordSchema.objects.create(**validated_data)
        else:
            raise serializers.ValidationError('Schema version could not be determined')
        return new

    class Meta:
        model = RecordSchema
        fields = '__all__'
        read_only_fields = ('uuid', 'next_version')


class BoundaryPolygonSerializer(GeoFeatureModelSerializer):
    data = JsonBField()

    class Meta:
        model = BoundaryPolygon
        geo_field = 'geom'
        id_field = 'uuid'
        exclude = ('boundary',)


class BoundaryPolygonNoGeomSerializer(ModelSerializer):
    """Serialize a BoundaryPolygon without any geometry info"""
    data = JsonBField()
    bbox = GeomBBoxField(source='geom')

    class Meta:
        model = BoundaryPolygon
        exclude = ('geom',)


class BoundarySerializer(GeoModelSerializer):

    # label = serializers.CharField(max_length=128, allow_blank=False)
    # color = serializers.CharField(max_length=64, required=False)
    # display_field = serializers.CharField(max_length=10, allow_blank=True, required=False)
    # data_fields = JsonBField(read_only=True, allow_null=True)
    # errors = JsonBField(read_only=True, allow_null=True)

    def create(self, validated_data):
        boundary = super(BoundarySerializer, self).create(validated_data)
        boundary.load_shapefile()
        return boundary

    class Meta:
        model = Boundary
        # These meta read_only/exclude settings only apply to the fields the ModelSerializer
        # instantiates for you by default. If you override a field manually, you need to override
        # all settings there.
        # e.g. adding 'errors' to this tuple has no effect, since we manually define the errors
        # field above
        read_only_fields = ('uuid', 'status',)
        fields = '__all__'


class DriverRecordSerializer(BaseDriverRecordSerializer):
    modified_by = SerializerMethodField(method_name='get_latest_change_email')

    def get_latest_change_email(self, record):
        """Returns the email of the user who has most recently modified this Record"""
        latest_audit_entry = (RecordAuditLogEntry.objects
                              .filter(record=record)
                              .order_by('-date')
                              .first())
        if latest_audit_entry:
            if latest_audit_entry.user is not None:
                return latest_audit_entry.user.email
            return latest_audit_entry.username
        return None


class DetailsReadOnlyRecordSerializer(BaseDriverRecordSerializer):
    """Serialize records with only read-only fields included"""
    data = serializer_fields.MethodTransformJsonField('filter_details_only')

    def filter_details_only(self, key, value):
        """Return only the details object and no other related info"""
        if re.search(settings.READ_ONLY_FIELDS_REGEX, key):
            return key, value
        else:
            raise serializer_fields.DropJsonKeyException


class DetailsReadOnlyRecordNonPublicSerializer(DetailsReadOnlyRecordSerializer):
    """
    Serialize records with only read-only fields included plus non-public fields
    (only available to admins and analysts)
    """
    created_by = CharField()


class DetailsReadOnlyRecordSchemaSerializer(serializers.RecordSchemaSerializer):
    """Serialize Schema with only read-only fields included"""
    schema = serializer_fields.MethodTransformJsonField('make_read_only_schema')

    def make_read_only_schema(self, key, value):
        if key != 'properties' and key != 'definitions':
            return key, value

        # If we're looking at properties/definitions, remove everything that isn't read-only
        new_value = {}
        for k in value.viewkeys():
            if re.search(settings.READ_ONLY_FIELDS_REGEX, k):
                new_value[k] = value[k]
        return key, new_value


class RecordAuditLogEntrySerializer(ModelSerializer):
    """Serialize Audit Log Entries"""
    record_url = SerializerMethodField()

    def get_record_url(self, obj):
        return settings.HOST_URL + '/record/{}/details'.format(str(obj.record_uuid))

    class Meta:
        model = RecordAuditLogEntry
        fields = ['date', 'username', 'action', 'record_url']


class RecordDuplicateSerializer(ModelSerializer):
    record = DriverRecordSerializer(required=False, allow_null=True)
    duplicate_record = DriverRecordSerializer(required=False, allow_null=True)

    class Meta:
        model = RecordDuplicate
        fields = '__all__'


class RecordCostConfigSerializer(ModelSerializer):
    def validate(self, data):
        """Check that the most recent schema for the record type matches the passed enum fields"""

        # Object-level validation and partial updates do not go well together:
        # https://github.com/tomchristie/django-rest-framework/issues/3070

        # Helper for getting the value of a key and falling back to the instance value if available
        def get_from_data(key):
            if self.instance:
                return data.get(key, getattr(self.instance, key))
            return data.get(key)

        cost_keys = set(get_from_data('enum_costs').keys())
        schema = get_from_data('record_type').get_current_schema()
        # TODO: This snippet also appears in data/views.py and should be refactored into the Grout
        # RecordSchema model
        path = [get_from_data('content_type_key'), 'properties', get_from_data('property_key')]
        obj = schema.schema['definitions']  # 'definitions' is the root of all schema paths
        for key in path:
            try:
                obj = obj[key]
            except KeyError:
                raise ValidationError("The property '{}' does not exist on the schema".format(key))
        items = obj.get('items', None)
        if items:
            choices = items.get('enum', None)
        else:
            choices = obj.get('enum', None)
        if not choices:
            raise ValidationError("The specified property must have choices (be an enum).")

        choice_keys = set(choices)
        if len(cost_keys.symmetric_difference(choice_keys)) != 0:
            raise ValidationError('The costs specified don\'t match the choices in the schema')
        return data

    class Meta:
        model = RecordCostConfig
        fields = '__all__'


class DriverRecordCopySerializer(serializers.RecordSerializer):
    class Meta:
        model = DriverRecordCopy
        fields = '__all__'


class WeatherInfoSerializer(serializers.ModelSerializer):
    class Meta:
        model = WeatherInfo
        fields = '__all__'


class DriverCrashDiagramSerializer(ModelSerializer):
    class Meta:
        model = CrashDiagramOrientation

        fields = ('uuid', 'crash_type', 'movement_code', 'image', 'is_active')


class DuplicateDistanceConfigSerializer(ModelSerializer):
    """Serializer for DedupeDistanceConfig object"""

    class Meta:
        model = DuplicateDistanceConfig
        fields = '__all__'


class BulkUploadDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = BulkUploadDetail
        fields = '__all__'


class DriverRecordMapSerializer(serializers.ModelSerializer):
    class Meta:
        model = DriverRecord
        fields = ('weather', 'light', 'uuid', 'occurred_from', 'geom', 'created', 'location_text')


class WeatherDataListSerializer(serializers.ModelSerializer):
    class Meta:
        model = WeatherDataList
        fields = '__all__'


class KeyDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = KeyDetail
        fields = ('keyname', 'value')
