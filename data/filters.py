import django_filters

from django.contrib.gis.db.models import Union
from django.contrib.gis.geos import GEOSGeometry
from django.core.exceptions import ValidationError
from django.db.models import Q

from django_redis import get_redis_connection
from rest_framework.exceptions import ParseError, NotFound

from data.models import RecordAuditLogEntry, RecordDuplicate
# from driver_auth.permissions import is_admin_or_writer
from data.models import DriverRecord, WeatherDataList
from grout.models import Boundary, BoundaryPolygon
from grout.filters import RecordFilter
from driver_advanced_auth.models import CountryInfo, LanguageDetail


class RecordAuditLogFilter(django_filters.FilterSet):
    """Allow filtering audit log entries by user, record, min_date, max_date"""
    min_date = django_filters.IsoDateTimeFilter(field_name="date", lookup_expr='gte')
    max_date = django_filters.IsoDateTimeFilter(field_name="date", lookup_expr='lte')
    action = django_filters.ChoiceFilter(choices=RecordAuditLogEntry.ActionTypes.choices)

    class Meta:
        model = RecordAuditLogEntry
        fields = ['record', 'record_uuid', 'action', 'min_date', 'max_date']


WEATHER_CHOICES = [(c, c) for c in [
    'clear-day',
    'clear-night',
    'cloudy',
    'fog',
    'hail',
    'partly-cloudy-day',
    'partly-cloudy-night',
    'rain',
    'sleet',
    'snow',
    'thunderstorm',
    'tornado',
    'wind',
    'thunderstorm with light rain',
    'thunderstorm with rain',
    'thunderstorm with heavy rain',
    'light thunderstorm',
    'thunderstorm',
    'heavy thunderstorm',
    'ragged thunderstorm',
    'thunderstorm with light drizzle',
    'thunderstorm with drizzle',
    'thunderstorm with heavy drizzle',
    'light intensity drizzle',
    'drizzle',
    'heavy intensity drizzle',
    'light intensity drizzle rain',
    'drizzle rain',
    'heavy intensity drizzle rain',
    'shower rain and drizzle',
    'heavy shower rain and drizzle',
    'shower drizzle',
    'light rain',
    'moderate rain',
    'heavy intensity rain',
    'very heavy rain',
    'extreme rain',
    'freezing rain',
    'light intensity shower rain',
    'shower rain',
    'heavy intensity shower rain',
    'ragged shower rain',
    'light snow',
    'Snow',
    'Heavy snow',
    'Sleet',
    'Light shower sleet',
    'Shower sleet',
    'Light rain and snow',
    'Rain and snow',
    'Light shower snow',
    'Shower snow',
    'Heavy shower snow',
    'mist',
    'Smoke',
    'Haze',
    'sand/ dust whirls',
    'fog',
    'sand',
    'dust',
    'volcanic ash',
    'squalls',
    'tornado',
    'clear sky',
    'few clouds: 11-25%',
    'scattered clouds: 25-50%',
    'broken clouds: 51-84%',
    'overcast clouds: 85-100%'
]]


class RecordDuplicateFilter(django_filters.FilterSet):
    record_type = django_filters.Filter(field_name='record_type', method='filter_record_type')
    occurred_min = django_filters.IsoDateTimeFilter(field_name='record__occurred_to', lookup_expr='gte')
    occurred_max = django_filters.IsoDateTimeFilter(field_name='record__occurred_from', lookup_expr='lte')
    search = django_filters.CharFilter(method='record_search_filter')
    polygon_id = django_filters.Filter(field_name='polygon_id', method='filter_polygon_id')
    reporting_agency = django_filters.CharFilter(method='reporting_agency_filter')

    def reporting_agency_filter(self, queryset, name, rep_agency):
        """
        for filtering potential duplicate records list based on reporting agency value.

        """
        r_agency = rep_agency.split(',')
        q = queryset.filter(record__data__driverIncidentDetails__contains={"Reporting Agency": r_agency[0]})
        for i in r_agency:
            q1 = queryset.filter(record__data__driverIncidentDetails__contains={"Reporting Agency": i})
            q = q1 | q
        return q

    def filter_polygon_id(self, queryset, field_name, poly_uuid):
        """Method filter for containment within the polygon using id"""
        try:
            return queryset.filter(record__geom__intersects=BoundaryPolygon.objects.get(pk=poly_uuid).geom)
        except ValueError as e:
            raise ParseError(e)
        except BoundaryPolygon.DoesNotExist as e:
            raise NotFound(e)

    def filter_record_type(self, queryset, value):
        """ Filter duplicates by the record type of their first record

        e.g. /api/duplicates/?record_type=44a51b83-470f-4e3d-b71b-e3770ec79772
        """
        return queryset.filter(record__schema__record_type=value)

    def record_search_filter(self, queryset, name, search, ):
        """
        for filtering potential duplicate records list based on
        texts of search box.

        """
        return queryset.filter(Q(record__weather__icontains=search)
                               | Q(record__light__icontains=search)
                               | Q(record__city__icontains=search)
                               | Q(record__state__icontains=search)
                               | Q(record__location_text__icontains=search))

    class Meta:
        model = RecordDuplicate
        fields = ['resolved', 'job', 'occurred_min', 'occurred_max', 'search']


class DriverRecordFilter(RecordFilter):
    """Extend RecordFilter to allow filtering on created date."""
    created_min = django_filters.IsoDateTimeFilter(field_name="created", lookup_expr='gte')
    created_max = django_filters.IsoDateTimeFilter(field_name="created", lookup_expr='lte')
    created_by = django_filters.Filter(field_name='created_by', method='filter_created_by')
    # weather = django_filters.MultipleChoiceFilter(choices=WEATHER_CHOICES)
    weather = django_filters.Filter(field_name='weather', method='filter_weather')
    archived = django_filters.BooleanFilter(field_name='archived')
    outside_boundary = django_filters.Filter(field_name='geom', method='filter_outside_boundary')
    location_text = django_filters.CharFilter(method='record_search_filter')
    identity = django_filters.CharFilter(field_name='identity', lookup_expr='icontains')

    def filter_weather(self, queryset, field_name, weather):
        weathers = weather.split(",")
        query = Q()
        for w in weathers:
            query.add(Q(weather=w), Q.OR)
        return queryset.filter(query)

    def __init__(self, data=None, *args, **kwargs):
        # if filterset is bound, use initial values as defaults
        if data is not None:
            # get a mutable copy of the QueryDict
            data = data.copy()

            if not data.get('archived'):
                data['archived'] = "False"

        super(DriverRecordFilter, self).__init__(data, *args, **kwargs)

    def filter_outside_boundary(self, queryset, field_name, boundary_uuid):
        """Filter records that fall outside the specified boundary."""
        redis_conn = get_redis_connection('boundaries')
        bounds_hexewkb = redis_conn.get(boundary_uuid)
        one_month_seconds = 30 * 24 * 60 * 60
        if bounds_hexewkb is None:
            try:
                boundary = Boundary.objects.get(pk=boundary_uuid)
            except ValidationError:
                raise ParseError('outside_boundary was passed an invalid UUID')
            except Boundary.DoesNotExist:
                raise NotFound('Boundary not found')
            unioned_bounds = boundary.polygons.aggregate(all_polys=Union('geom'))['all_polys']
            # Full resolution is very slow, so simplify down to roughly 100m (DRIVER is in lat/lon).
            unioned_bounds = unioned_bounds.simplify(tolerance=0.001, preserve_topology=True)
            redis_conn.set(boundary_uuid, str(unioned_bounds.hexewkb), one_month_seconds)
        else:
            redis_conn.expire(boundary_uuid, one_month_seconds)
            unioned_bounds = GEOSGeometry(bounds_hexewkb)
        return queryset.exclude(geom__intersects=unioned_bounds)

    def filter_created_by(self, queryset, name, value):
        """ Filter records by the email or username of the creating user."""
        if not (self.request.user):
            # Public users cannot filter by creating user
            return queryset

        return queryset.filter(
            Q(recordauditlogentry__action=RecordAuditLogEntry.ActionTypes.CREATE) &
            (Q(recordauditlogentry__username=value) |
             Q(recordauditlogentry__user__email=value))
        )

    def record_search_filter(self, queryset, name, search, ):
        """
        for filtering records based on location text search bar.

        """
        return queryset.filter(location_text__icontains=search)

    class Meta:
        model = DriverRecord
        fields = ['created_min', 'created_max', 'identity']


class CountryInfoFilter(django_filters.FilterSet):
    archived = django_filters.BooleanFilter(field_name='archived', method='archived_filter')

    def archived_filter(self, queryset, name, value):
        return queryset.filter(archived=value)

    class Meta:
        model = CountryInfo
        fields = ['archived']


UPLOAD_FOR_CHOICES = [(c, c) for c in [
    'user_panel',
    'admin_panel']
                      ]


class LanguageDetailFilter(django_filters.FilterSet):
    archive = django_filters.BooleanFilter(field_name='archive', method='is_archive_filter')
    upload_for = django_filters.ChoiceFilter(choices=UPLOAD_FOR_CHOICES)

    def is_archive_filter(self, queryset, name, value):
        return queryset.filter(Q(archive=value) |
                               Q(upload_for=value))

    class Meta:
        model = LanguageDetail
        fields = ['archive', 'upload_for']


class WeatherDataListFilter(django_filters.FilterSet):
    active = django_filters.BooleanFilter(field_name='active', method='is_active_filter')

    def is_active_filter(self, queryset, name, value):
        return queryset.filter(active=value)

    class Meta:
        model = WeatherDataList
        fields = ['active']
