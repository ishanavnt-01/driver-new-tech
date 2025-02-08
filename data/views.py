import calendar
import copy
import datetime
import hashlib
import json
import logging
import operator
import os
import uuid
from functools import reduce

import pandas as pd
from collections import defaultdict
from datetime import timedelta
from zipfile import ZipFile
import pytz
import requests
from celery import states
from dateutil.parser import parse as parse_date
from django.conf import settings
from django.core import serializers
from django.db import connections
from django.db import transaction
from django.db.models import (
    Case,
    CharField,
    Count,
    IntegerField,
    OuterRef,
    Q,
    Subquery,
    Sum,
    UUIDField,
    Value,
    When,
)
from django.db.models.functions import Coalesce
from django.http import Http404
from django.template.defaultfilters import date as template_date
from django_redis import get_redis_connection
from grout.models import RecordSchema, RecordType, BoundaryPolygon, Boundary, Record
from grout.pagination import OptionalLimitOffsetPagination
from grout.serializers import (BoundarySerializer,
                               BoundaryPolygonSerializer,
                               RecordTypeSerializer)
from grout.serializers import RecordSchemaSerializer
from grout.views import (RecordViewSet,
                         RecordSchemaViewSet,
                         BoundaryViewSet)
from rest_framework import mixins as drf_mixins
from rest_framework import status
from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.exceptions import ParseError
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.settings import api_settings
from rest_framework.views import APIView
from rest_framework_csv import renderers as csv_renderer

from DRIVER import mixins
from data import filters
# from driver_auth.permissions import (IsAdminOrReadOnly,
#                                      ReadersReadWritersWrite,
#                                      IsAdminAndReadOnly,
#                                      is_admin_or_writer)
from data.tasks import export_csv
from .models import RecordAuditLogEntry, RecordDuplicate, RecordCostConfig, CrashDiagramOrientation, BulkUploadDetail, \
    DriverRecord, IrapDetail, WeatherInfo, DuplicateDistanceConfig, WeatherDataList, KeyDetail
from .serializers import (DriverRecordSerializer, DetailsReadOnlyRecordSerializer,
                          DetailsReadOnlyRecordSchemaSerializer, RecordAuditLogEntrySerializer,
                          RecordDuplicateSerializer, RecordCostConfigSerializer,
                          DetailsReadOnlyRecordNonPublicSerializer, DriverCrashDiagramSerializer,
                          DuplicateDistanceConfigSerializer, BulkUploadDetailSerializer, DriverRecordMapSerializer,
                          WeatherDataListSerializer, KeyDetailSerializer)

# from data.localization.date_utils import (
#     hijri_day_range,
#     hijri_week_range,
#     hijri_month_range,
#     hijri_year_range
# )
from django.db.models.expressions import RawSQL
logger = logging.getLogger(__name__)

from rest_framework.filters import SearchFilter

# DateTimeField.register_lookup(transformers.ISOYearTransform)
# DateTimeField.register_lookup(transformers.WeekTransform)

xrange = range


def build_toddow(queryset):
    """
    Builds a toddow object

    :param queryset: Queryset of records
    """
    # Build SQL `case` statement to annotate with the day of week
    dow_case = Case(*[When(occurred_from__week_day=x, then=Value(x))
                      for x in xrange(1, 8)], output_field=IntegerField())
    # Build SQL `case` statement to annotate with the time of day
    tod_case = Case(*[When(occurred_from__hour=x, then=Value(x))
                      for x in xrange(24)], output_field=IntegerField())
    annotated_recs = queryset.annotate(dow=dow_case).annotate(tod=tod_case)
    # Voodoo to perform aggregations over `tod` and `dow` combinations
    return (annotated_recs.values('tod', 'dow')
            .order_by('tod', 'dow')
            .annotate(count=Count('tod')))


class DriverRecordViewSet(RecordViewSet, mixins.GenerateViewsetQuery):
    """Override base RecordViewSet from grout to provide aggregation and tiler integration
    """
    permissions_classes = [IsAuthenticated, ]
    # filter_backends = (SearchFilter,)
    queryset = DriverRecord.objects.all()
    # filter_class = filters.DriverRecordFilter
    filterset_class = filters.DriverRecordFilter
    pagination_class = OptionalLimitOffsetPagination

    # Filter out everything except details for read-only users
    def get_serializer_class(self):
        # check if parameter details_only is set to true, and if so, use details-only serializer
        requested_details_only = False
        details_only_param = self.request.query_params.get('details_only', None)
        if details_only_param == 'True' or details_only_param == 'true':
            requested_details_only = True

        if (self.request.user):
            if requested_details_only:
                return DetailsReadOnlyRecordNonPublicSerializer
            else:
                return DriverRecordSerializer
        return DetailsReadOnlyRecordSerializer

    def get_queryset(self):
        qs = super(DriverRecordViewSet, self).get_queryset()
        if self.get_serializer_class() is DetailsReadOnlyRecordNonPublicSerializer:
            # Add in `created_by` field for user who created the record
            created_by_query = (
                RecordAuditLogEntry.objects.filter(
                    record=OuterRef('pk'),
                    action=RecordAuditLogEntry.ActionTypes.CREATE
                )
                    .annotate(
                    # Fall back to username if the user has been deleted
                    email_or_username=Coalesce('user__email', 'username')
                )
                    .values('email_or_username')
                [:1]
            )
            qs = qs.annotate(created_by=Subquery(created_by_query, output_field=CharField()))
        return qs.order_by('-occurred_from')

    def get_filtered_queryset(self, request):
        """Return the queryset with the filter backends applied. Handy for aggregations."""
        queryset = self.get_queryset()
        for backend in list(self.filter_backends):
            queryset = backend().filter_queryset(request, queryset, self)
        return queryset

    def add_to_audit_log(self, request, instance, action):
        """Creates a new audit log entry; instance must have an ID"""
        if not instance.pk:
            raise ValueError('Cannot create audit log entries for unsaved model objects')
        if action not in RecordAuditLogEntry.ActionTypes.as_list():
            raise ValueError("{} not one of 'create', 'update', or 'delete'".format(action))
        log = None
        signature = None
        if action == RecordAuditLogEntry.ActionTypes.CREATE:
            log = serializers.serialize(
                'json',
                [
                    DriverRecord.objects.get(pk=instance.pk),
                    Record.objects.get(pk=instance.record_ptr_id)
                ]
            )
            # signature = hashlib.md5(log).hexdigest()
            signature = hashlib.sha256(log.encode('utf-8')).hexdigest()
        RecordAuditLogEntry.objects.create(
            user=request.user,
            username=request.user.username,
            record=instance,
            record_uuid=str(instance.pk),
            action=action,
            log=log,
            signature=signature
        )

    @transaction.atomic
    def perform_update(self, serializer):
        lat_lon = []
        if serializer.validated_data.get('weather') == "" or serializer.validated_data.get('light') == "" or \
                serializer.validated_data.get('weather') == "null" and serializer.validated_data.get('light') == "null":
            try:
                weather = WeatherInfo.objects.get(is_active=True)
                weather_info_obj = WeatherInfo.objects.get(is_active=True)
                provider_name = weather_info_obj.provider_name
                for g in serializer.validated_data.get('geom'):
                    lat_lon.append(g)
                    instance = serializer.save()
            except WeatherInfo.DoesNotExist:
                instance = serializer.save()
        else:
            instance = serializer.save()
        self.add_to_audit_log(self.request, instance, RecordAuditLogEntry.ActionTypes.UPDATE)

    @transaction.atomic
    def perform_create(self, serializer):
        lat_lon = []
        if serializer.validated_data.get('weather') == "" or serializer.validated_data.get('light') == "" or \
                serializer.validated_data.get('weather') == "null" and serializer.validated_data.get('light') == "null":
            try:
                weather = WeatherInfo.objects.get(is_active=True)
                weather_info_obj = WeatherInfo.objects.get(is_active=True)
                provider_name = weather_info_obj.provider_name
                for coordinates in serializer.validated_data.get('geom'):
                    lat_lon.append(coordinates)
                    instance = serializer.save()
                    self.add_to_audit_log(self.request, instance, RecordAuditLogEntry.ActionTypes.CREATE)
            except WeatherInfo.DoesNotExist:
                instance = serializer.save()
                self.add_to_audit_log(self.request, instance, RecordAuditLogEntry.ActionTypes.CREATE)
        else:
            instance = serializer.save()
            self.add_to_audit_log(self.request, instance, RecordAuditLogEntry.ActionTypes.CREATE)

    @transaction.atomic
    def perform_destroy(self, instance):
        self.add_to_audit_log(self.request, instance, RecordAuditLogEntry.ActionTypes.DELETE)
        instance.delete()

    # Views
    def list(self, request, *args, **kwargs):
        # Don't generate a tile key unless the user specifically requests it, to avoid
        # filling up the Redis cache with queries that will never be viewed as tiles
        if ('tilekey' in request.query_params and
                request.query_params['tilekey'] in ['True', 'true']):
            response = Response(dict())
            query_sql = self.generate_query_sql(request)
            tile_token = str(uuid.uuid4())
            self._cache_tile_sql(tile_token, query_sql)
            response.data['tilekey'] = tile_token
        else:
            response = super(DriverRecordViewSet, self).list(self, request, *args, **kwargs)
        return response

    def _cache_tile_sql(self, token, sql):
        """Stores a sql string in the common cache so it can be retrieved by Windshaft later"""
        # We need to use a raw Redis connection because the Django cache backend
        # transforms the keys and values before storing them. If the cached data
        # were being read by Django, this transformation would be reversed, but
        # since the stored sql will be parsed by Windshaft / Postgres, we need
        # to store the data exactly as it is.
        redis_conn = get_redis_connection('default')
        # redis_conn.set(token, sql.encode('utf-8'))
        redis_conn.set(str(token), sql)

    @action(methods=['get'], detail=False)
    def stepwise(self, request):
        """Return an aggregation counts the occurrence of events per week (per year) between
        two bounding datetimes
        e.g. [{"week":35,"count":13,"year":2015},{"week":43,"count":1,"year":2015}]
        """
        # We'll need to have minimum and maximum dates specified to properly construct our SQL
        try:
            start_date = parse_date(request.query_params['occurred_min'])
            end_date = parse_date(request.query_params['occurred_max'])
        except KeyError:
            raise ParseError("occurred_min and occurred_max must both be provided")
        except ValueError:
            raise ParseError("occurred_min and occurred_max must both be valid dates")

        # The min year can't be after or more than 2000 years before the max year
        year_distance = end_date.year - start_date.year
        if year_distance < 0:
            raise ParseError("occurred_min must be an earlier date than occurred_max")
        if year_distance > 2000:
            raise ParseError("occurred_min and occurred_max must be within 2000 years of one another")

        queryset = self.get_filtered_queryset(request)

        # Build SQL `case` statement to annotate with the year
        isoyear_case = Case(*[When(occurred_from__isoyear=year, then=Value(year))
                              for year in range(start_date.year, end_date.year + 1)],
                            output_field=IntegerField())
        # Build SQL `case` statement to annotate with the day of week
        week_case = Case(*[When(occurred_from__week=week, then=Value(week))
                           for week in xrange(1, 54)],
                         output_field=IntegerField())

        annotated_recs = queryset.annotate(year=isoyear_case).annotate(week=week_case)

        # Voodoo to perform aggregations over `week` and `year` combinations
        counted = (annotated_recs.values('week', 'year')
                   .order_by('week', 'year')
                   .annotate(count=Count('week')))

        return Response(counted)

    @action(methods=['get'], detail=False)
    def toddow(self, request):
        """ Return aggregations which nicely format the counts for time of day and day of week
        e.g. [{"count":1,"dow":6,"tod":1},{"count":1,"dow":3,"tod":3}]
        """
        queryset = self.get_filtered_queryset(request)
        counted = build_toddow(queryset)
        return Response(counted)

    @action(methods=['get'], detail=False)
    def recent_counts(self, request):
        """ Return the recent record counts for 30, 90, 365 days """
        now = datetime.datetime.now(tz=pytz.timezone(settings.TIME_ZONE))
        qs = self.get_filtered_queryset(request).filter(occurred_from__lte=now)
        durations = {
            'month': 30,
            'quarter': 90,
            'year': 365,
        }

        counts = {label: qs.filter(occurred_from__gte=(now - datetime.timedelta(days=days))).count()
                  for label, days in durations.items()}
        return Response(counts)

    @action(methods=['get'], detail=False)
    def costs(self, request):
        """Return the costs for a set of records of a certain type

        This endpoint requires the record_type query parameter. All other query parameters will be
        used to filter the queryset before calculating costs.

        There must be a RecordCostConfig associated with the RecordType passed, otherwise a 404
        will be returned. If there are multiple RecordCostConfigs associated with a RecordType, the
        most recently created one will be used.

        Uses the most recent schema for the RecordType; if this doesn't match the fields in the
        RecordCostConfig associated with the RecordType, an exception may be raised.

        Returns a response of the form:
        {
            total: X,
            subtotals: {
                enum_choice1: A,
                enum_choice2: B,
                ...
            }
        }
        """
        record_type_id = request.query_params.get('record_type', None)
        if not record_type_id:
            raise ParseError(detail="The 'record_type' parameter is required")
        cost_config = (RecordCostConfig.objects.filter(record_type_id=record_type_id)
                       .order_by('-created').first())
        if not cost_config:
            return Response({'record_type': 'No cost configuration found for this record type.'},
                            status=status.HTTP_404_NOT_FOUND)
        schema = RecordType.objects.get(pk=record_type_id).get_current_schema()
        path = cost_config.path
        multiple = self._is_multiple(schema, path)
        choices = self._get_schema_enum_choices(schema, path)
        # `choices` may include user-entered data; to prevent users from entering column names
        # that conflict with existing Record fields, we're going to use each choice's index as an
        # alias instead.
        choice_indices = {str(idx): choice for idx, choice in enumerate(choices)}
        counts_queryset = self.get_filtered_queryset(request)
        for idx, choice in choice_indices.items():
            filter_rule = self._make_djsonb_containment_filter(path, choice, multiple)
            # We want a column for each enum choice with a binary 1/0 indication of whether the row
            # in question has that enum choice. This is to support checkbox fields which can have
            # more than one selection from the enum per field. Then we're going to sum those to get
            # aggregate counts for each enum choice.
            choice_case = Case(When(data__jsonb=filter_rule, then=Value(1)), default=Value(0),
                               output_field=IntegerField())
            annotate_params = dict()
            annotate_params[idx] = choice_case
            counts_queryset = counts_queryset.annotate(**annotate_params)
        output_data = {'prefix': cost_config.cost_prefix,
                       'suffix': cost_config.cost_suffix if cost_config.cost_suffix != None else ""}
        if not counts_queryset:  # Short-circuit if no events at all
            output_data.update({"total": 0, "subtotals": {choice: 0 for choice in choices},
                                "outdated_cost_config": False})
            return Response(output_data)
        # Do the summation
        sum_ops = [Sum(key) for key in choice_indices.keys()]
        sum_qs = counts_queryset.values(*choice_indices.keys()).aggregate(*sum_ops)
        # sum_qs will now look something like this: {'0__sum': 20, '1__sum': 45, ...}
        # so now we need to slot in the corresponding label from `choices` by pulling the
        # corresponding value out of choices_indices.
        sums = {}
        for key, choice_sum in sum_qs.items():
            index = key.split('_')[0]
            choice = choice_indices[index]
            sums[choice] = choice_sum
        # Multiply sums by per-incident costs to get subtotal costs broken out by type
        subtotals = dict()
        # This is going to be extremely easy for users to break if they update a schema without
        # updating the corresponding cost configuration; if things are out of sync, degrade
        # gracefully by providing zeroes for keys that don't match and set a flag so that the
        # front-end can alert users if needed.
        found_missing_choices = False
        for key, value in sums.items():
            try:
                subtotals[key] = value * int(cost_config.enum_costs[key])
            except KeyError:
                logger.warning('Schema and RecordCostConfig out of sync; %s missing from cost config',
                               key)
                found_missing_choices = True
                subtotals[key] = 0
        total = sum(subtotals.values())
        # Return breakdown costs and sum
        output_data.update({'total': f"{total:,d}", 'subtotals': subtotals,
                            'outdated_cost_config': found_missing_choices})
        return Response(output_data)

    @action(methods=['get'], detail=False)
    def crosstabs(self, request):
        """Returns a columnar aggregation of event totals; this is essentially a generalized ToDDoW

        Requires the following query parameters:
        - Exactly one row specification parameter chosen from:
            - row_period_type: A time period to use as rows; valid choices are:
                               {'hour', 'day', 'week_day', 'week', 'month', 'year'}
                               The value 'day' signifies day-of-month
            - row_boundary_id: Id of a Boundary whose BoundaryPolygons should be used as rows
            - row_choices_path: Path components to a schema property whose choices should be used
                                as rows, separated by commas
                                e.g. &row_choices_path=incidentDetails,properties,Collision%20type
                                Note that ONLY properties which have an 'enum' key are valid here.
        - Exactly one column specification parameter chosen from:
            - col_period_type
            - col_boundary_id
            - col_choices_path
            As you might expect, these operate identically to the row_* parameters above, but the
            results are used as columns instead.
        - record_type: The UUID of the record type which should be aggregated
        - calendar: the calendar to use for the report to (Ex: 'gregorian' or 'ummalqura')

        Allows the following query parameters:
        - aggregation_boundary: Id of a Boundary; separate tables will be generated for each
                                BoundaryPolygon associated with the Boundary.
        - all other filter params accepted by the list endpoint; these will filter the set of
            records before any aggregation is applied. This may result in some rows / columns /
            tables having zero records.

        Note that the tables are sparse: rows will only appear in 'data' and 'row_totals',
        and columns will only appear in rows, if there are values returned by the query.
        'row_labels' and 'col_labels', however, are complete and in order.

        Response format:
        {
            "row_labels": [
                { "key": "row_key1", "label": "row_label1"},
                ...
            ],
            "col_labels": [
                { "key": "col_key1", "label": "col_label1"},
                ...
            ],
            "table_labels": {
                "table_key1": "table_label1",
                // This will be empty if aggregation_boundary is not provided
            },
            "tables": [
                {
                    "tablekey": "table_key1",
                    "data": {
                        {
                            "row_key1": {
                                "col_key1": N,
                                "col_key3": N,
                            },
                        },
                        ...
                    },
                    "row_totals": {
                        {
                            "row_key1": N,
                            "row_key3": N,
                    }
                },
                ...
            ]
        }
        """
        valid_row_params = set(['row_period_type', 'row_boundary_id', 'row_choices_path'])
        valid_col_params = set(['col_period_type', 'col_boundary_id', 'col_choices_path'])
        # Validate there's exactly one row_* and one col_* parameter
        row_params = set(request.query_params) & valid_row_params
        col_params = set(request.query_params) & valid_col_params
        upload_for = request.query_params.get('data_for')
        collabel = request.query_params.get('colLabel')
        rowlabel = request.query_params.get('rowLabel')
        if len(row_params) != 1 or len(col_params) != 1:
            raise ParseError(detail='Exactly one col_* and row_* parameter required; options are {}'
                             .format(list(valid_row_params | valid_col_params)))

        # Get queryset, pre-filtered based on other params
        queryset = self.get_filtered_queryset(request)

        # Pass parameters to case-statement generators
        row_param = row_params.pop()  # Guaranteed to be just one at this point
        col_param = col_params.pop()
        row_multi, row_labels, annotated_qs = self._query_param_to_annotated_tuple(
            row_param, request, queryset, 'row')
        col_multi, col_labels, annotated_qs = self._query_param_to_annotated_tuple(
            col_param, request, annotated_qs, 'col')

        week_count = 0
        labels = []
        month_list = ["JANUARY", "SEPTEMBER", "AUGUST", "JULY", "JUNE", "MAY", "APRIL", "MARCH", "FEBRUARY", "OCTOBER",
                      "NOVEMBER", "DECEMBER"]
        new_list = []

        for i in row_labels:
            week_count += 1
            for item in i['label']:
                if "." in item['text']:
                    colval = item['text'].split(".")[1]
                    if colval in ["week", "WEEK"]:
                        item['text'] = colval + " " + str(week_count)
                    else:
                        item['text'] = colval
                else:
                    if rowlabel == 'Week':
                        var_new = item['text'].startswith(tuple(month_list))
                        if not var_new:
                            new_list.append(i)
                        row_labels = new_list

        week_count_col = 0
        for i in col_labels:
            labels.append(i['label'][0]['text'])
            week_count_col += 1
            for item in i['label']:
                if "." in item['text']:
                    colval = item['text'].split(".")[1]
                    if colval in ["week", "WEEK"]:
                        item['text'] = colval + " " + str(week_count_col)
                    else:
                        item['text'] = colval
                else:
                    if collabel == 'Week':
                        var_new = item['text'].startswith(tuple(month_list))
                        if not var_new:
                            new_list.append(i)
                        col_labels = new_list

        # If aggregation_boundary_id exists, grab the associated BoundaryPolygons.
        tables_boundary = request.query_params.get('aggregation_boundary', None)
        if tables_boundary:
            boundaries = BoundaryPolygon.objects.filter(boundary=tables_boundary)
        else:
            boundaries = None

        # Assemble crosstabs either once or multiple times if there are boundaries
        response = dict(tables=[], table_labels=dict(), row_labels=row_labels,
                        col_labels=col_labels)
        if boundaries:
            # Add table labels
            parent = Boundary.objects.get(pk=tables_boundary)
            response['table_labels'] = {str(poly.pk): poly.data[parent.display_field]
                                        for poly in boundaries}
            # Filter by polygon for counting
            for poly in boundaries:
                table = self._fill_table(
                    annotated_qs.filter(geom__within=poly.geom),
                    row_multi, row_labels, col_multi, col_labels)
                table['tablekey'] = poly.pk
                response['tables'].append(table)
        else:
            response['tables'].append(self._fill_table(annotated_qs, row_multi, row_labels, col_multi, col_labels))

        if upload_for == 'barchart':
            col_keys = []
            row_keys = []
            x_axis_list = []
            y_axis_list = []
            final_value_list = []
            for i in range(len(response['col_labels'])):
                col_keys.append(response['col_labels'][i]['key'])
                x_axis_list.append(response['col_labels'][i]['label'][0]['text'])
            for j in range(len(response['row_labels'])):
                row_keys.append(response['row_labels'][j]['key'])
                y_axis_list.append(response['row_labels'][j]['label'][0]['text'])
            for row_key in row_keys:
                initial_value_list = []
                for col_key in col_keys:
                    initial_value_list.append(response['tables'][0]['data'][row_key][col_key])
                final_value_list.append(initial_value_list)
            return Response({'x-axis': x_axis_list, 'y-axis': y_axis_list, 'data': final_value_list})
        elif upload_for == 'piechart':
            col_keys = []
            row_keys = []
            x_axis_list = []
            y_axis_list = []
            final_value_dict = {}
            response_key_dict = {}
            for i in range(len(response['col_labels'])):
                col_keys.append(response['col_labels'][i]['key'])
                x_axis_list.append(response['col_labels'][i]['label'][0]['text'])
            for j in range(len(response['row_labels'])):
                row_keys.append(response['row_labels'][j]['key'])
                y_axis_list.append(response['row_labels'][j]['label'][0]['text'])
            for x_axis in x_axis_list:
                response_key_dict[x_axis] = 0
            for row_key in row_keys:
                for col_key in col_keys:
                        if col_key in final_value_dict.keys():
                            final_value_dict[col_key] += response['tables'][0]['data'][row_key][col_key]
                        else:
                            final_value_dict[col_key] = response['tables'][0]['data'][row_key][col_key]
            return Response({'x-axis': response_key_dict.keys(), 'data': final_value_dict.values()})
        else:
            return Response(response)

    def _fill_table(self, annotated_qs, row_multi, row_labels, col_multi, col_labels):
        """ Fill a nested dictionary with the counts and compute row totals. """
        # The data being returned is a nested dictionary: row label -> col labels = integer count
        data = defaultdict(lambda: defaultdict(int))
        if not row_multi and not col_multi:
            # Not in multi-mode: sum rows/columns by a simple count annotation.
            # This is the normal case.
            # Note: order_by is necessary here -- it's what triggers django to do a group by.
            for value in (annotated_qs.values('row', 'col')
                    .order_by('row', 'col')
                    .annotate(count=Count('row'))):
                if value['row'] is not None and value['col'] is not None:
                    data[str(value['row'])][str(value['col'])] = value['count']
        elif row_multi and col_multi:
            # The row and column are in multi-mode, iterate to build up counts.
            # This is a very rare case, since creating a report between two 'multiple' items
            # doesn't seem very useful, at least with the current set of data. We may even end
            # up restricting this via the front-end. But until then, it's been implemented
            # here, and it works, but is on the slow side, since it needs to manually aggregate.


            # updated after python upgrade.
            def sanitize_key(value):
                """Sanitize keys to match annotation naming."""
                return str(value).replace(' ', '_').replace('"', '').replace(';', '').replace('-', '_')

            for record in annotated_qs:
                rd = record.__dict__
                row_ids = [
                    str(label['key'])
                    for label in row_labels
                    if rd.get('row_{}'.format(sanitize_key(label['key'])), 0) > 0
                ]
                col_ids = [
                    str(label['key'])
                    for label in col_labels
                    if rd.get('col_{}'.format(sanitize_key(label['key'])), 0) > 0
                ]

                # Each object has row_* and col_* fields, where a value > 0 indicates presence.
                # Increment the counter for each combination.
                for row_id in row_ids:
                    for col_id in col_ids:
                        data[row_id][col_id] += 1
        else:
            # Either the row or column is a 'multiple' item, but not both.
            # This is a relatively common case and is still very fast since the heavy-lifting
            # is all done within the db.
            if row_multi:
                multi_labels = row_labels
                single_label = 'col'
                multi_prefix = 'row'
            else:
                multi_labels = col_labels
                single_label = 'row'
                multi_prefix = 'col'

            multi_labels = [
                '{}_{}'.format(multi_prefix, str(label['key']))
                for label in multi_labels
            ]
            # Perform a sum on each of the 'multi' columns, storing the data in a sum_* field

            annotated_qs = (
                annotated_qs.values(single_label, *multi_labels)
                    .order_by()
                    .annotate(**{"sum_%s" % (label): Sum(label) for label in multi_labels})
            )

            # Iterate over each object and accumulate each sum in the proper dictionary position.
            # Each object either has a 'row' and several 'col_*'s or a 'col' and several 'row_*'s.
            # Get the combinations accordingly and accumulate the appropriate stored value.

            for rd in annotated_qs:
                for multi_label in multi_labels:
                    sum_val = rd['sum_{}'.format(multi_label)]
                    rd_row = rd['row'] if 'row' in rd else 'None'
                    rd_col = rd['col'] if 'col' in rd else 'None'

                    if row_multi:
                        data[str(multi_label[4:])][str(rd_col)] += sum_val
                    else:
                        data[str(rd_row)][str(multi_label[4:])] += sum_val

        row_totals = {row: sum(cols.values()) for (row, cols) in list(data.items())}
        return {'data': data, 'row_totals': row_totals}

    def _get_annotated_tuple(self, queryset, annotation_id, case, labels):
        """Helper wrapper for annotating a queryset with a case statement

        Args:
          queryset (QuerySet): The input queryset
          annotation_id (String): 'row' or 'col'
          case (Case): The generated Case statement
          labels (dict<Case, String>): dict mapping Case values to labels

        Returns:
            A 3-tuple of:
              - boolean which specifies whether or not this is a 'multiple' query (always False)
              - dict mapping Case values to labels
              - the newly-annotated queryset
        """
        kwargs = {}
        kwargs[annotation_id] = case
        annotated_qs = queryset.annotate(**kwargs)
        return (False, labels, annotated_qs)

    def _query_param_to_annotated_tuple(self, param, request, queryset, annotation_id):
        """Wrapper to handle getting the params for each case generator because we do it twice. TODO....."""
        try:
            record_type_id = request.query_params['record_type']
        except KeyError:
            raise ParseError(detail="The 'record_type' parameter is required")

        if param.endswith('period_type'):
            query_calendar = request.query_params.get('calendar')
            if (query_calendar == 'gregorian'):
                return self._get_annotated_tuple(
                    queryset, annotation_id,
                    *self._make_gregorian_period_case(
                        request.query_params[param], request, queryset))
            elif (query_calendar == 'ummalqura'):
                return self._get_annotated_tuple(
                    queryset, annotation_id,
                    *self._make_ummalqura_period_case(
                        request.query_params[param], request, queryset))
        elif param.endswith('boundary_id'):
            return self._get_annotated_tuple(
                queryset, annotation_id, *self._make_boundary_case(request.query_params[param]))
        else:  # 'choices_path'; ensured by parent function
            schema = RecordType.objects.get(pk=record_type_id).get_current_schema()
            path = request.query_params[param].split(',')
            return self._get_multiple_choices_annotated_tuple(
                queryset, annotation_id, schema, path)

    def _get_day_label(self, week_day_index):
        """Constructs a day translation label string given a week day index

        Args:
            week_day_index (int): Django `week_day` property (1-indexed, starting with Sunday)

        Returns:
            A string representing the day translation label
        """
        # week_day is 1-indexed and starts with Sunday, whereas day_name
        # is 0-indexed and starts with Monday, so we need to map indices as follows:
        # 1,2,3,4,5,6,7 -> 6,0,1,2,3,4,5 for Sunday through Saturday
        return 'DAY.{}'.format(
            calendar.day_name[6 if week_day_index == 1 else week_day_index - 2].upper()
        )

    def _make_gregorian_period_case(self, period_type, request, queryset):
        """Constructs a Django Case statement for a certain type of period.

        Args:
            period_type (string): one of the valid aggegation type keys, either periodic (e.g.
                'day_of_week', 'month_of_year') or sequential (e.g. 'day', 'month', 'year')
            request (Request): the request, from which max and min date will be read if needed
            queryset (QuerySet): filtered queryset to use for getting date range if it's needed
                and the request is missing a max and/or min date
        Returns:
            (Case, labels), where Case is a Django Case object giving the period in which each
            record's occurred_from falls, and labels is a dict mapping Case values to period labels
        """
        # Most date-related things are 1-indexed.
        # TODO: these dates will need to be localized (which will include passing in the language).
        periodic_ranges = {
            'month_of_year': {
                'range': xrange(1, 13),
                'lookup': lambda x: {'occurred_from__month': x},
                'label': lambda x: [
                    {
                        'text': 'MONTH.{}'.format(calendar.month_name[x].upper()),
                        'translate': True
                    }
                ]
            },
            'week_of_year': {
                'range': xrange(1, 54),  # Up to 53 weeks in a year
                'lookup': lambda x: {'occurred_from__week': x},
                'label': lambda x: [
                    {
                        'text': 'AGG.WEEK',
                        'translate': True
                    },
                    {
                        'text': str(x),
                        'translate': False
                    }
                ]
            },
            'day_of_week': {
                'range': xrange(1, 8),
                'lookup': lambda x: {'occurred_from__week_day': x},
                'label': lambda x: [
                    {
                        'text': self._get_day_label(x),
                        'translate': True
                    }
                ]
            },
            'day_of_month': {
                'range': xrange(1, 32),
                'lookup': lambda x: {'occurred_from__day': x},
                'label': lambda x: [
                    {
                        'text': str(x),
                        'translate': False
                    }
                ]
            },
            'hour_of_day': {
                'range': xrange(0, 24),
                'lookup': lambda x: {'occurred_from__hour': x},
                'label': lambda x: [
                    {
                        'text': '{}:00'.format(x),
                        'translate': False
                    }
                ]
            },
        }

        # Ranges are built below, partly based on the ranges in 'periodic_ranges' above.
        sequential_ranges = {
            'year': {
                'range': [],
                'lookup': lambda x: {'occurred_from__year': x},
                'label': lambda x: [
                    {
                        'text': str(x),
                        'translate': False
                    }
                ]
            },
            'month': {
                'range': [],
                # 'lookup': lambda (yr, mo): {'occurred_from__month': mo, 'occurred_from__year': yr},
                'lookup': lambda yr, mo: {'occurred_from__month': mo, 'occurred_from__year': yr},
                # 'label': lambda (yr, mo): [
                'label': lambda yr, mo: [
                    {
                        'text': '{}, {}'.format(calendar.month_name[mo], str(yr)),
                        'translate': False
                    }
                ]
            },
            'week': {
                'range': [],
                # 'lookup': lambda (yr, wk): {'occurred_from__week': wk, 'occurred_from__year': yr},
                'lookup': lambda yr, wk: {'occurred_from__week': wk, 'occurred_from__year': yr},
                # 'label': lambda (yr, wk): [
                'label': lambda yr, wk: [
                    {
                        'text': str(yr),
                        'translate': False
                    },
                    {
                        'text': 'AGG.WEEK',
                        'translate': True
                    },
                    {
                        'text': str(wk),
                        'translate': False
                    }
                ]
            },
            'day': {
                'range': [],
                # 'lookup': lambda (yr, mo, day): {'occurred_from__month': mo,
                'lookup': lambda yr, mo, day: {'occurred_from__month': mo,
                                               'occurred_from__year': yr,
                                               'occurred_from__day': day},
                # 'label': lambda (yr, mo, day): [
                'label': lambda yr, mo, day: [
                    {
                        'text': template_date(datetime.date(yr, mo, day)),
                        'translate': False
                    }
                ]
            },
        }

        if period_type in periodic_ranges.keys():
            period = periodic_ranges[period_type]
        elif period_type in sequential_ranges.keys():
            # Get the desired range, either from the query params or the filtered queryset
            if request.query_params.get('occurred_min') is not None:
                min_date = parse_date(request.query_params['occurred_min']).date()
            else:
                min_date = queryset.order_by('occurred_from').first().occurred_from.date()
            if request.query_params.get('occurred_max') is not None:
                max_date = parse_date(request.query_params['occurred_max']).date()
            else:
                max_date = queryset.order_by('-occurred_from').first().occurred_from.date()

            # Build the relevant range of aggregation periods, based partly on the ones
            # already built in 'periodic_ranges' above
            sequential_ranges['year']['range'] = xrange(min_date.year, max_date.year + 1)
            if period_type != 'year':
                # Using the existing lists for 'year' and 'month_of_year', builds a list of
                # (year, month) tuples in order for the min_date to max_date range
                sequential_ranges['month']['range'] = [
                    (year, month) for year in sequential_ranges['year']['range']
                    for month in periodic_ranges['month_of_year']['range']
                    if min_date <= datetime.date(year, month, calendar.monthrange(year, month)[1])
                       and datetime.date(year, month, 1) <= max_date
                ]
                if period_type == 'day':
                    # Loops over the 'month' range from directly above and adds day, to make a
                    # list of (year, month, day) tuples in order for the min_date to max_date range
                    sequential_ranges['day']['range'] = [
                        (year, month, day) for (year, month) in sequential_ranges['month']['range']
                        for day in xrange(1, calendar.monthrange(year, month)[1] + 1)
                        if min_date <= datetime.date(year, month, day)
                           and datetime.date(year, month, day) <= max_date
                    ]
                elif period_type == 'week':
                    # Using the existing lists for 'year' and 'week_of_year', builds a list of
                    # (year, week) tuples in order for the min_date to max_date range.
                    # Figure out what week the min_date and max_date fall in, then
                    # use them as the starting and ending weeks
                    def week_start_date(year, week):
                        d = datetime.date(year, 1, 1)
                        delta_days = d.isoweekday() - 1
                        delta_weeks = week
                        if year == d.isocalendar()[0]:
                            delta_weeks -= 1
                        delta = datetime.timedelta(days=-delta_days, weeks=delta_weeks)
                        return d + delta

                    sequential_ranges['week']['range'] = [
                        (year, week) for year in sequential_ranges['year']['range']
                        for week in periodic_ranges['week_of_year']['range']
                        if week_start_date(
                            min_date.year, min_date.isocalendar()[1]
                        ) <= week_start_date(year, week)  # include first partial week
                           and week_start_date(year, week) <= max_date  # include last partial week
                    ]

            period = sequential_ranges[period_type]
        else:
            raise ParseError(detail=('row_/col_period_type must be one of {}; received {}'
                                     # .format(periodic_ranges.keys() + sequential_ranges.keys(),
                                     .format(tuple(periodic_ranges.keys()) + tuple(sequential_ranges.keys()),
                                             period_type)))

        return self._build_case_from_period(period)

    def _make_ummalqura_period_case(self, period_type, request, queryset):
        periodic_ranges = {
            'month_of_year': {
                'type': 'generated',
                'query': hijri_month_range,
            },
            'week_of_year': {
                'type': 'generated',
                'query': hijri_week_range
            },
            'day_of_week': {
                'type': 'builtin',
                'range': xrange(1, 8),
                'lookup': lambda x: {'occurred_from__week_day': x},
                'label': lambda x: [
                    {
                        'text': self._get_day_label(x),
                        'translate': True
                    }
                ]
            },
            'day_of_month': {
                'type': 'generated',
                'query': hijri_day_range,
            },
            'hour_of_day': {
                'type': 'builtin',
                'range': xrange(0, 24),
                'lookup': lambda x: {'occurred_from__hour': x},
                'label': lambda x: [
                    {
                        'text': '{}:00'.format(x),
                        'translate': False
                    }
                ]
            },
        }

        # Ranges are built below, partly based on the ranges in 'periodic_ranges' above.
        sequential_ranges = {
            'year': {
                'type': 'generated',
                'query': hijri_year_range
            },
            'month': {
                'type': 'generated',
                'query': hijri_month_range
            },
            'week': {
                'type': 'generated',
                'query': hijri_week_range
            },
            'day': {
                'type': 'generated',
                'query': hijri_day_range
            },
        }

        # need to get start/end of every month in the requested range
        # create Q expressions for each month
        # create aggregation for each type of Case query which doesn't translate directly from
        # the gregorian calendar:
        # Periodic: Day of Month, Month of Year, Week of year
        # Sequential: Day, Month, Year

        # Min / max dates are required to limit the # of Q expressions
        if request.query_params.get('occurred_min') is not None:
            min_date = parse_date(request.query_params['occurred_min']).date()
        else:
            min_date = queryset.order_by('occurred_from').first().occurred_from.date()
        if request.query_params.get('occurred_max') is not None:
            max_date = parse_date(request.query_params['occurred_max']).date()
        else:
            max_date = queryset.order_by('-occurred_from').first().occurred_from.date()

        if period_type in periodic_ranges.keys():
            return self._build_ummalqura_periodic_case(
                periodic_ranges, period_type, min_date, max_date
            )
        elif period_type in sequential_ranges.keys():
            return self._build_ummalqura_sequential_case(
                sequential_ranges, period_type, min_date, max_date
            )
        else:
            raise ParseError(detail=('row_/col_period_type must be one of {}; received {}'
                                     .format(periodic_ranges.keys() + sequential_ranges.keys(),
                                             period_type)))

    def _build_ummalqura_periodic_case(self, periodic_ranges, period_type, min_date, max_date):
        period = periodic_ranges[period_type]

        if period['type'] == 'generated':
            query_dates = period['query'](min_date, max_date, True)
            date_sets = query_dates['date_sets']

            whens = []
            labels = []

            for date_set in date_sets:
                range_expressions = []
                for date_range in date_set.ranges:
                    range_expressions.append(
                        (Q(occurred_from__gte=date_range.start) &
                         Q(occurred_from__lt=date_range.end))
                    )
                if len(range_expressions) > 1:
                    in_range = reduce(lambda x, y: x | y, range_expressions)
                elif len(range_expressions) == 1:
                    in_range = range_expressions[0]
                else:
                    continue
                set_when = When(in_range, then=Value(date_set.key))
                whens.append(set_when)
                labels.append({'key': date_set.key,
                               'label': date_set.label})
            return (Case(*whens, output_field=CharField()), labels)

        elif period['type'] == 'builtin':
            return self._build_case_from_period(period)

    def _build_case_from_period(self, period):
        whens = []  # Eventual list of When-clause objects

        for x in period['range']:
            try:
                when_args = period['lookup'](x)
                when_args['then'] = Value(str(x))
                whens.append(When(**when_args))
            except:
                if len(x) == 3:
                    # Day
                    when_args = period['lookup'](x[0], x[1], x[2])
                else:
                    # Month / Week
                    when_args = period['lookup'](x[0], x[1])

                when_args['then'] = Value(str(x))
                whens.append(When(**when_args))
                # pass
                # continue
        try:
            labels = [{'key': str(x), 'label': period['label'](x)} for x in period['range']]
        except:
            # for Day
            monthlist = ["JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY", "AUGUST", "SEPTEMBER",
                         "OCTOBER", "NOVEMBER", "DECEMBER"]

            labels = []
            if len(period['range'][0]) == 2:
                try:
                    # Month
                    for x in period['range']:
                        key = str(x)
                        data = [{"text": monthlist[x[1] - 1] + ", " + str(x[0]), "translate": False}]
                        labels.append({'key': key, 'label': data})
                except:
                    # WEEK
                    for x in period['range']:
                        key = str(x)
                        data = [{"text": str(x[0]) + " WEEK " + str(x[1]), "translate": True}]
                        labels.append({'key': key, 'label': data})
                        pass
            elif len(period['range'][0]) == 3:
                # Day
                try:
                    labels = []
                    for x in period['range']:
                        key = str(x)
                        data = [
                            {"text": monthlist[x[1] - 1] + " " + str(x[2]) + " " + str(x[0]), "translate": False}]
                        labels.append({'key': key, 'label': data})
                except:
                    pass

        return (Case(*whens, output_field=CharField()), labels)

    def _build_ummalqura_sequential_case(self, sequential_ranges, period_type, min_date, max_date):
        period = sequential_ranges[period_type]
        if period and period['type'] == 'generated':
            query_dates = period['query'](min_date, max_date)
            date_sets = query_dates['date_sets']

            whens = []
            labels = []

            for date_set in date_sets:
                # only ever 1 range for each sequential when
                date_range = date_set.ranges[0]
                range_expression = (
                        Q(occurred_from__gte=date_range.start) &
                        Q(occurred_from__lt=date_range.end))
                set_when = When(range_expression, then=Value(date_set.key))
                whens.append(set_when)
                labels.append({'key': date_set.key,
                               'label': date_set.label})
            return (Case(*whens, output_field=CharField()), labels)

        else:
            raise ParseError(
                description='Invalid sequential aggregations type for ummalqura calendar'
            )

    def _make_boundary_case(self, boundary_id):
        """Constructs a Django Case statement for points falling within a particular polygon

        Args:
            boundary_id (uuid): Id of a Boundary whose BoundaryPolygons should be used in the case
        Returns:
            (Case, labels), where Case is a Django Case object outputting the UUID of the polygon
            which contains each record, and labels is a dict mapping boundary pks to their labels.
        """
        boundary = Boundary.objects.get(pk=boundary_id)
        polygons = BoundaryPolygon.objects.filter(boundary=boundary)

        # Sort the polygons by display_field and remove any items that have an empty label
        polygons = sorted([p for p in polygons if p.data[boundary.display_field]],
                          key=lambda p: p.data[boundary.display_field])
        labels = [
            {
                'key': str(poly.pk),
                'label': [
                    {'text': poly.data[boundary.display_field], 'translate': False}
                ]
            }
            for poly in polygons
        ]

        return (Case(*[When(geom__within=poly.geom, then=Value(poly.pk)) for poly in polygons],
                     output_field=UUIDField()), labels)

    def _is_multiple(self, schema, path):
        """Determines whether this related object type has a multiple item configuration

        Args:
            schema (RecordSchema): A RecordSchema to get properties from
            path (list): A list of path fragments to navigate to the desired property
        Returns:
            True if this related object type has a multiple item configuration
        """
        # The related key is always the first item appearing in the path
        try:
            if 'multiple' not in schema.schema['definitions'][path[0]]:
                return False
            return schema.schema['definitions'][path[0]]['multiple']
        except:
            # This shouldn't ever fail, but in case a bug causes the schema to change, treat
            # the related type as non-multiple, since that's the main use-case
            logger.exception('Exception obtaining multiple with path: %s', path)
            return False

    def _make_choices_case(self, schema, path):
        """Constructs a Django Case statement for the choices of a schema property

        Args:
            schema (RecordSchema): A RecordSchema to get properties from
            path (list): A list of path fragments to navigate to the desired property
        Returns:
            (Case, labels), where Case is a Django Case object with the choice of each record,
            and labels is a dict matching choices to their labels (currently the same).
        """

        multiple = self._is_multiple(schema, path)
        choices = self._get_schema_enum_choices(schema, path)
        whens = []
        for choice in choices:
            filter_rule = self._make_djsonb_containment_filter(path, choice, multiple)
            whens.append(When(data__jsonb=filter_rule, then=Value(choice)))
        labels = [
            {'key': choice, 'label': [{'text': choice, 'translate': False}]}
            for choice in choices
        ]
        return (Case(*whens, output_field=CharField()), labels)

    def _get_multiple_choices_annotated_tuple(self, queryset, annotation_id, schema, path):
        """Helper wrapper for annotating a queryset with a case statement

        Args:
          queryset (QuerySet): The input queryset
          annotation_id (String): 'row' or 'col'
          schema (RecordSchema): A RecordSchema to get properties from
          path (list): A list of path fragments to navigate to the desired property

        Returns:
            A 3-tuple of:
              - boolean which specifies whether or not this is a 'multiple' query (always False)
              - dict mapping Case values to labels
              - the newly-annotated queryset
        """

        choices = self._get_schema_enum_choices(schema, path)

        labels = [
            {'key': choice, 'label': [{'text': choice, 'translate': False}]}
            for choice in choices
        ]

        is_array = self._is_multiple(schema, path)
        annotations = {}
        for choice in choices:
            sanitized_choice = str(choice).replace(' ', '_').replace('"', '').replace(';', '').replace('-', '_')

            if is_array:
                pattern = json.dumps({path[2]: choice})
                pattern = pattern[1:len(pattern) - 1]
                annotations[f'{annotation_id}_{sanitized_choice}'] = RawSQL(
                    """
                    SELECT count(*) 
                    FROM regexp_matches("grout_record"."data"->>%s, %s, 'g')
                    """, (path[0], pattern)
                )

            else:
                expression = "data__%s__%s__contains" % (path[0], path[2])

                annotations[f'{annotation_id}_{sanitized_choice}'] = Case(
                    When(**{expression: choice}, then=Value(1)),
                    output_field=IntegerField(),
                    default=Value(0)
                )

        return (True, labels, queryset.annotate(**annotations))

    # TODO: This snippet also appears in data/serializers.py and should be refactored into the Grout
    # RecordSchema model
    def _get_schema_enum_choices(self, schema, path):
        """Returns the choices in a schema enum field at path

        Args:
            schema (RecordSchema): A RecordSchema to get properties from
            path (list): A list of path fragments to navigate to the desired property
        Returns:
            choices, where choices is a list of strings representing the valid values of the enum.
        """
        # Walk down the schema using the path components
        obj = schema.schema['definitions']  # 'definitions' is the root of all schema paths
        for key in path:
            try:
                obj = obj[key]
            except KeyError as e:
                raise ParseError(
                    detail=u'Could not look up path "{}", "{}" was not found in schema'.format(
                        u':'.join(path), key))

        # Checkbox types have an additional 'items' part at the end of the path
        if 'items' in obj:
            obj = obj['items']

        # Build a JSONB filter that will catch Records that match each choice in the enum.
        choices = obj.get('enum', None)
        if not choices:
            raise ParseError(detail="The property at choices_path is missing required 'enum' field")
        return choices

    def _make_djsonb_containment_filter(self, path, value, multiple):
        """Returns a djsonb containment filter for a path to contain a value

        Args:
            path (list): A list of strings denoting the path
            value (String): The value to match on
            multiple (Boolean): True if this related object type has a multiple item configuration
        Returns:
            A dict representing a valid djsonb containment filter specification that matches if the
            field at path contains value
        """
        # Build the djsonb filter specification from the inside out, and skip schema-only keys, i.e.
        # 'properties' and 'items'.
        filter_path = [component for component in reversed(path)
                       if component not in ['properties', 'items']]

        # Check if a row contains either the value as a string, or the value in an array.
        # The string value handles dropdown types, while the array handles checkbox types.
        # Since an admin may switch between dropdowns and checkboxes at any time, performing
        # both checks guarantees the filter will be correctly applied for data in both formats.
        rule_type = 'containment_multiple' if multiple else 'containment'
        filter_rule = dict(_rule_type=rule_type, contains=[value, [value]])
        for component in filter_path:
            # Nest filter inside itself so we eventually get something like:
            # {"incidentDetails": {"severity": {"_rule_type": "containment"}}}
            tmp = dict()
            tmp[component] = filter_rule
            filter_rule = tmp
        return filter_rule


class DriverRecordAuditLogViewSet(viewsets.ModelViewSet):
    """Viewset for accessing audit logs; will output CSVs if Accept text/csv is specified"""
    queryset = RecordAuditLogEntry.objects.all()
    renderer_classes = api_settings.DEFAULT_RENDERER_CLASSES + [csv_renderer.CSVRenderer]
    serializer_class = RecordAuditLogEntrySerializer
    permission_classes = []
    filter_class = filters.RecordAuditLogFilter
    pagination_class = None

    def list(self, request, *args, **kwargs):
        """Validate filter params"""
        # Will throw an error if these are missing or not valid ISO-8601
        try:
            min_date = parse_date(request.query_params['min_date'])
            max_date = parse_date(request.query_params['max_date'])
        except KeyError:
            raise ParseError("min_date and max_date must both be provided")
        except ValueError:
            raise ParseError("occurred_min and occurred_max must both be valid dates")
        # Make sure that min_date and max_date are less than 32 days apart
        if max_date - min_date >= datetime.timedelta(days=32):
            raise ParseError('max_date and min_date must be less than one month apart')
        return super(DriverRecordAuditLogViewSet, self).list(request, *args, **kwargs)

    # Override default CSV field ordering and include URL
    def get_renderer_context(self):
        context = super(DriverRecordAuditLogViewSet, self).get_renderer_context()
        context['header'] = self.serializer_class.Meta.fields
        return context


def start_jar_build(schema_uuid):
    """Helper to kick off build of a Dalvik jar file with model classes for a schema.
    Publishes schema with its UUID to a redis channel that the build task listens on.

    :param schema_uuid: Schema UUID, which is the key used to store the jar on redis.
    """
    # Find the schema with the requested UUID.
    schema_model = RecordSchema.objects.get(uuid=schema_uuid)
    if not schema_model:
        return False

    schema = schema_model.schema
    json_schema = json.dumps({'uuid': schema_uuid, 'schema': schema})
    redis_conn = get_redis_connection('jars')
    redis_conn.publish('jar-build', json.dumps({'uuid': schema_uuid, 'schema': schema}))
    return True


class DriverRecordTypeViewSet(viewsets.ModelViewSet):
    queryset = RecordType.objects.filter(active=True)
    serializer_class = RecordTypeSerializer
    # filter_class = RecordTypeFilter
    pagination_class = None
    ordering = ('plural_label',)


class RecordTypeViewSet(APIView):

    def get(self, request):
        label = self.request.query_params.get('label', None)
        # active = self.request.query_params.get('active', None)
        queryset = RecordType.objects.filter(label=label)
        serializer = RecordTypeSerializer(queryset, many=True)
        return Response({"results": serializer.data, "status": 200})

        # all_organizations = Organization.objects.all()
        # for region in region_list:
        #     for rgn in region["uuids"].split(','):
        #         queryset = all_organizations.filter(region_id=rgn)
        #         query_list.extend(queryset)
        # serializer = CitySerializer(query_list, many=True)
        # return Response([{"data":serializer.data,"status":"True"}], status=200)


class DriverRecordSchemaViewSet(RecordSchemaViewSet):
    # permission_classes = (IsAdminOrReadOnly,)
    pagination_class = None

    # Filter out everything except details for read-only users
    def get_serializer_class(self):
        if (self.request.user):
            return RecordSchemaSerializer
        return DetailsReadOnlyRecordSchemaSerializer

    def perform_create(self, serializer):
        instance = serializer.save()
        start_jar_build(str(instance.pk))

    def perform_update(self, serializer):
        instance = serializer.save()
        start_jar_build(str(instance.pk))

    def perform_destroy(self, instance):
        redis_conn = get_redis_connection('jars')
        redis_conn.delete(str(instance.pk))
        instance.delete()


# class DriverBoundaryViewSet(BoundaryViewSet):
#     permission_classes = (IsAdminOrReadOnly,)


class DriverRecordDuplicateViewSet(viewsets.ModelViewSet):
    duplicate_object = RecordDuplicate.objects.all().exclude(Q(record=None) |
                                                             Q(duplicate_record=None))
    queryset = duplicate_object.order_by('record__occurred_to')
    serializer_class = RecordDuplicateSerializer
    filter_class = filters.RecordDuplicateFilter
    permissions_classes = [IsAuthenticated, ]

    @action(methods=['patch'], detail=True)
    def resolve(self, request, pk=None):
        duplicate = self.queryset.get(pk=pk)

        recordUUID = request.data.get('recordUUID', None)

        if recordUUID is None:

            # No record id means they want to keep both, so just resolve the duplicate
            duplicate.resolved = True
            duplicate.save()
            resolved_ids = [duplicate.pk]

        else:

            # If they picked a record, archive the other one and resolve all duplicates involving it
            # (which will include the current one)
            if recordUUID == str(duplicate.record.uuid):
                rejected_record = duplicate.duplicate_record
            elif recordUUID == str(duplicate.duplicate_record.uuid):
                rejected_record = duplicate.record
            else:
                raise Exception("Error: Trying to resolve a duplicate with an unconnected record.")
            rejected_record.archived = True
            rejected_record.save()
            resolved_dup_qs = RecordDuplicate.objects.filter(Q(resolved=False),
                                                             Q(record=rejected_record) |
                                                             Q(duplicate_record=rejected_record))
            resolved_ids = [str(uuid) for uuid in resolved_dup_qs.values_list('pk', flat=True)]
            resolved_dup_qs.update(resolved=True)
        return Response({'resolved': resolved_ids})


class DriverRecordCostConfigViewSet(viewsets.ModelViewSet):
    queryset = RecordCostConfig.objects.all()
    serializer_class = RecordCostConfigSerializer
    pagination_class = None
    filter_fields = ('record_type',)


class RecordCsvExportViewSet(viewsets.ViewSet):
    pagination_class = None
    """A view for interacting with CSV export jobs

    Since these jobs are not model-backed, we won't use any of the standard DRF mixins
    """

    # permissions_classes = (IsAdminOrReadOnly,)

    def retrieve(self, request, pk=None):
        """Return the status of the celery task with query_params['taskid']"""
        # N.B. Celery will never return an error if a task_id doesn't correspond to a
        # real task; it will simply return a task with a status of 'PENDING' that will never
        # complete.
        job_result = export_csv.AsyncResult(pk)
        if job_result.state in states.READY_STATES:
            if job_result.state in states.EXCEPTION_STATES:
                e = job_result.get(propagate=False)
                return Response({'status': job_result.state, 'error': str(e)})
            # Set up the URL to proxy to the celery worker
            # TODO: This won't work with multiple celery workers
            # TODO: We should add a cleanup task to prevent result files from accumulating
            # on the celery worker.
            url = str(settings.HOST_URL)
            scheme = url.split('/')[0]
            uri = u'{scheme}//{host}{prefix}{download}{file}'.format(scheme=scheme,
                                                            host=request.get_host(),
                                                            prefix=settings.CELERY_DOWNLOAD_PREFIX,
                                                            download='download/',
                                                            file=job_result.get())
            return Response({'status': job_result.state, 'result': uri})
        return Response({'status': job_result.state, 'info': job_result.info})

    def create(self, request, *args, **kwargs):
        """Create a new CSV export task, using the passed filterkey as a parameter

        filterkey is the same as the "tilekey" that we pass to Windshaft; it must be requested
        from the Records endpoint using tilekey=true
        """

        filter_key = request.data.get('tilekey', None)
        group_id = request.data.get('group_id')
        if not filter_key:
            return Response({'errors': {'tilekey': 'This parameter is required'}},
                            status=status.HTTP_400_BAD_REQUEST)

        task = export_csv.delay(filter_key, request.user.pk, group_id)

        return Response({'success': True, 'taskid': task.id}, status=status.HTTP_201_CREATED)
        # filter_key = request.data.get('tilekey', None)
        # if not filter_key:
        #     return Response({'errors': {'tilekey': 'This parameter is required'}},
        #                     status=status.HTTP_400_BAD_REQUEST)

        # task = export_csv.delay(filter_key, request.user.pk)
        # return Response({'success': True, 'taskid': task.id}, status=status.HTTP_201_CREATED)

    # TODO: If we switch to a Django/ORM database backend, we can subclass AbortableTask
    # and allow cancellation as well.

from django.http import JsonResponse
class GetLatestRecordSchema(APIView):
    def post(self, request):

        with connections['default'].cursor() as cursor:
            record_type_id = request.data['record_type_id']
            query = """
                SELECT * FROM grout_recordschema
                WHERE record_type_id::text = %s
                ORDER BY created DESC
                LIMIT 1
            """
            cursor.execute(query, [str(record_type_id)])
            desc = cursor.description
            results = [
                dict(zip([col[0] for col in desc], row))
                for row in cursor.fetchall()
            ]

        # Deserialize JSONField values
        for result in results:
            for key, value in result.items():
                if isinstance(value, str):  # Check if the value might be JSON
                    try:
                        result[key] = json.loads(value)
                    except (json.JSONDecodeError, TypeError):
                        pass  # Not a JSON string, leave it as is

        return JsonResponse({"result": results})


class RecordCost(APIView):
    def post(self, request, format=None):
        serializer = RecordCostConfigSerializer(data=request.data, context={'request': request})
        if serializer.is_valid():
            serializer.save()

            return Response(serializer.data, status=status.HTTP_201_CREATED)

        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class GetLatestRecordcost(APIView):

    def get(self, request):
        queryset = RecordCostConfig.objects.all()
        serializer = RecordCostConfigSerializer(queryset, many=True)
        return Response({"data": serializer.data, "status": "true"}, status=200)

    def post(self, request):
        with connections['default'].cursor() as cursor:
            record_type_id = request.data['record_type_id']
            query = "select *  from data_recordcostconfig  where record_type_id::text = " + "'" + str(
                record_type_id) + "'" + " ORDER BY created DESC LIMIT 1"

            cursor.execute(query)
            desc = cursor.description
            results = [
                dict(zip([col[0] for col in desc], row))
                for row in cursor.fetchall()
            ]

        return Response({"result": results})  # get permission by Group


# Change the version of the cost aggrigation
class UpdateRecordConfig(APIView):

    def get(self, request):
        with connections['default'].cursor() as cursor:
            record_type_id = request.data['record_type_id']

            query = "select *  from data_recordcostconfig  where record_type_id::text = " + "'" + str(
                record_type_id) + "'" + " ORDER BY created DESC LIMIT 1"
            cursor.execute(query)
            desc = cursor.description
            results = [
                dict(zip([col[0] for col in desc], row))
                for row in cursor.fetchall()
            ]

        return Response({"result": results})  # get permission by Group


# Create JSONB structure for text search on the map view

class CreateJsonViewSet(APIView):

    def post(self, request, uuids, headers=None):
        base_url = str(settings.HOST_URL) + "/data-api/latestrecordschema/"
        token = request.META.get('HTTP_AUTHORIZATION')
        response = requests.post(base_url,
                                 data={"record_type_id": uuids},
                                 headers={"Authorization": token}
                                 )
        rectype_id = response.json()["result"][0]['uuid']

        queryset = RecordSchema.objects.filter(uuid=rectype_id)
        serializer = RecordSchemaSerializer(queryset, many=True)
        req_value = request.data['req_search']
        datadict = {}
        for key_parent, value_parent in (serializer.data[0]["schema"]["definitions"]).items():
            nesteddict = {}
            array = serializer.data[0]["schema"]["definitions"][key_parent]["properties"]
            if "multiple" in serializer.data[0]["schema"]["definitions"][key_parent]:
                if serializer.data[0]["schema"]["definitions"][key_parent]["multiple"] == False:
                    containment = "containment"
                else:
                    containment = "containment_multiple"

            else:
                containment = "containment"

            for k, v in array.items():
                if "isSearchable" in v:
                    if v["isSearchable"] == True:
                        # new_dict[k] = v
                        nesteddict[k] = {"pattern": req_value, "_rule_type": containment}
                        datadict[key_parent] = nesteddict
                else:
                    pass
        return Response(datadict)

# old driver python3.8
# class WeeklyBarGraph(APIView):
#
#     def get(self, request, headers=None):
#
#         occured_min = parse_date(request.query_params['occurred_min'])
#         occured_max = parse_date(request.query_params['occurred_max'])
#         jsonb = request.query_params['jsonb']
#         record_type = str(request.query_params['record_type'])
#         location_text = request.query_params['location_text'].upper()
#
#         # weather = request.query_params['weather']
#         if len(location_text) > 0:
#             location_text1 = location_text.split(' ')
#             location_text2 = "-/".join(location_text1)
#             location_text3 = location_text2.split(',')
#             location_text4 = "-/".join(location_text3)
#             location_text5 = location_text4.split('-/')
#             location_text6 = []
#             for x in location_text5:
#                 location_text6.append("'%" + x + "%'")
#
#             location_text7 = " or UPPER(public.grout_record.location_text) LIKE ".join(location_text6)
#
#         else:
#             location_text7 = "'" + '%%' + "'"
#
#         if len(jsonb) > 0:
#             jsonb1 = jsonb.split(' ')
#             jsonb2 = "-/".join(jsonb1)
#             jsonb3 = jsonb2.split(',')
#             jsonb4 = "-/".join(jsonb3)
#             jsonb5 = jsonb4.split('-/')
#             jsonb6 = []
#             for x in jsonb5:
#                 jsonb6.append("'%" + x + "%'")
#
#             jsonb7 = " and CAST(public.grout_record.data as varchar(2000)) LIKE ".join(jsonb6)
#
#         else:
#             jsonb7 = "'" + '%%' + "'"
#         """ ImP code starts from here"""
#         total_count = []
#
#         start_dates = [occured_min]
#         end_dates = []
#         today = occured_min
#         while today <= occured_max:
#             tomorrow = today + timedelta(days=7)
#             #  if tomorrow.month != today.month:
#             start_dates.append(tomorrow)
#             end_dates.append(today)
#             today = tomorrow
#
#         end_dates.append(occured_max)
#
#         for eachdate in (end_dates):
#             nextdate = eachdate + timedelta(days=7)
#             nextdate = nextdate
#
#             if nextdate <= occured_max:
#                 with connections['default'].cursor() as cursor:
#                     # ami's code
#                     # query = "select * from grout_record where (occurred_from between " + "'" + str(
#                     #     eachdate) + "'" + " and " + "'" + str(nextdate) + "'" + ") and (" + inputs + ")"
#                     # query = "select * from grout_record where (occurred_from between " + "'" + str(
#                     #     eachdate) + "'" + " and " + "'" + str(nextdate) + "'" + ")"
#                     query = "select public.grout_record.uuid from public.grout_record inner join public.data_driverrecord on public.grout_record.uuid = public.data_driverrecord.record_ptr_id inner join public.grout_recordschema on public.grout_record.schema_id = public.grout_recordschema.next_version_id inner join public.grout_recordtype on public.grout_recordschema.record_type_id = public.grout_recordtype.uuid where (SELECT CAST(public.grout_record.data as varchar(2000)) LIKE " + jsonb7 + " ) and (SELECT UPPER(public.grout_record.location_text) LIKE " + str(
#                         location_text7) + ") and (public.grout_record.created between " + "'" + str(
#                         eachdate) + "'" + " and " + "'" + str(
#                         nextdate) + "'" + ") and public.grout_record.archived = false and public.grout_recordtype.uuid = " + "'" + record_type + "'"
#                     # query = "select distinct(public.grout_record.uuid) from public.grout_record inner join public.grout_recordschema on public.grout_record.schema_id = public.grout_recordschema.next_version_id inner join public.grout_recordtype on public.grout_recordschema.record_type_id = public.grout_recordtype.uuid where (public.grout_record.created between " + "'" + str(
#                     #     eachdate) + "'" + " and " + "'" + str(nextdate) + "'" + ") and public.grout_record.archived = false and public.grout_recordtype.uuid = "+"'"+record_type+"'"
#
#                     cursor.execute(query)
#                     # cursor.execute(query1)
#                     # row1 = cursor.fetchall()
#                     # desc = cursor.description
#                     columns = (x.name for x in cursor.description)
#                     row = cursor.fetchall()
#                     if row == []:
#                         # if row1[0][0] == 0:
#                         count = 0
#
#                     else:
#                         # result = dict(zip(columns, row))
#                         count = len(row)
#                         #   count = row1[0][0]
#
#                     total_count.append({"Week of " + str(eachdate): count})
#
#         return Response(total_count)

# new python 3.13
class WeeklyBarGraph(APIView):

    def get(self, request, headers=None):
        occured_min = parse_date(request.query_params['occurred_min'])
        occured_max = parse_date(request.query_params['occurred_max'])
        jsonb = request.query_params['jsonb']
        record_type = str(request.query_params['record_type'])
        location_text = request.query_params['location_text'].upper()

        # Build LIKE patterns
        location_text_like = f"%{location_text}%" if location_text else "%%"
        jsonb_like = f"%{jsonb}%" if jsonb else "%%"

        total_count = []
        week_ranges = [(occured_min + timedelta(days=7 * i), occured_min + timedelta(days=7 * (i + 1)))
                       for i in range((occured_max - occured_min).days // 7)]

        for start_date, end_date in week_ranges:
            with connections['default'].cursor() as cursor:
                query = """
                SELECT COUNT(public.grout_record.uuid) 
                FROM public.grout_record 
                INNER JOIN public.data_driverrecord 
                    ON public.grout_record.uuid = public.data_driverrecord.record_ptr_id 
                INNER JOIN public.grout_recordschema 
                    ON public.grout_record.schema_id = public.grout_recordschema.next_version_id 
                INNER JOIN public.grout_recordtype 
                    ON public.grout_recordschema.record_type_id = public.grout_recordtype.uuid 
                WHERE CAST(public.grout_record.data AS varchar(2000)) LIKE %s 
                  AND UPPER(public.grout_record.location_text) LIKE %s 
                  AND public.grout_record.created BETWEEN %s AND %s 
                  AND public.grout_record.archived = false 
                  AND public.grout_recordtype.uuid = %s
                """
                cursor.execute(query, [jsonb_like, location_text_like, start_date, end_date, record_type])
                count = cursor.fetchone()[0]
                total_count.append({f"Week of {start_date}": count})

        return Response(total_count)



# Bind enum parameters of Crashdiagram

class BindCrashTypeViewSet(APIView):

    def get(self, request, uuids, headers=None):
        nested_filter = []
        base_url = str(settings.HOST_URL) + "/data-api/latestrecordschema/"
        token = request.META.get('HTTP_AUTHORIZATION')
        response = requests.post(base_url,
                                 data={"record_type_id": uuids},
                                 headers={"Authorization": token}
                                 )
        rectype_id = response.json()["result"][0]['uuid']

        queryset = RecordSchema.objects.filter(uuid=rectype_id)
        serializer = RecordSchemaSerializer(queryset, many=True)

        for key_parent, value_parent in (serializer.data[0]["schema"]["definitions"]).items():

            array = serializer.data[0]["schema"]["definitions"][key_parent]["properties"]
            for (key, value) in array.items():
                nested_filter.append({key: value})

        return Response(nested_filter)


# Add movement type and Crash predefined images
class CreateCrashDiagramViewset(APIView):
    def post(self, request):
        serializer = DriverCrashDiagramSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response([{"Data": serializer.data, "message": "Data saved successfully", "status": True}],
                            status=status.HTTP_201_CREATED)
        return Response([{"Data": serializer.errors, "message": "error", "status": False}],
                        status=status.HTTP_400_BAD_REQUEST)

    def get(self, request):
        appand_data = []
        if (request.GET.get('crash_type') == '' and request.GET.get('uuid') == ''):
            queryset = CrashDiagramOrientation.objects.filter(is_active=request.GET.get('is_active'))
            serializer = DriverCrashDiagramSerializer(queryset, many=True)
            if serializers:
                for eachrecord in serializer.data:
                    appand_data.append({"uuid": eachrecord["uuid"], "crash_type": eachrecord["crash_type"],
                                        "movement_code": eachrecord["movement_code"],
                                        "is_active": eachrecord["is_active"]})
                return Response(appand_data, status=200)

            return Response([{"Data": serializer.errors, "message": "error", "status": False}],
                            status=status.HTTP_400_BAD_REQUEST)

        elif (request.GET.get('uuid') == ''):
            queryset = CrashDiagramOrientation.objects.filter(crash_type=request.GET.get('crash_type'), is_active=True)
        elif (request.GET.get('crash_type') == ''):
            queryset = CrashDiagramOrientation.objects.filter(uuid=request.GET.get('uuid'), is_active=True)
        else:
            queryset = CrashDiagramOrientation.objects.filter(crash_type=request.GET.get('crash_type'),
                                                              uuid=request.GET.get('uuid'), is_active=True)
        serializer = DriverCrashDiagramSerializer(queryset, many=True)
        if serializers:
            return Response(serializer.data, status=200)

        return Response([{"Data": serializer.errors, "message": "error", "status": False}],
                        status=status.HTTP_400_BAD_REQUEST)


# Get  movement type and Crash predefined images from Crash type
class GetCrashDiagramOrientationViewset(APIView):

    def get(self, request):
        queryset = CrashDiagramOrientation.objects.filter(crash_type=request.GET.get('crash_type'),
                                                          movement_code=request.GET.get('movement_code'),
                                                          is_active=True)
        serializer = DriverCrashDiagramSerializer(queryset, many=True)
        if serializers:
            return Response([{"Data": serializer.data, "status": True}], status=200)

        return Response([{"Data": serializer.errors, "message": "error", "status": False}],
                        status=status.HTTP_400_BAD_REQUEST)


# Update movement code and Images
class UpdateCrashDiagramViewset(APIView):
    def get_object(self, uuids=None):

        try:
            return CrashDiagramOrientation.objects.get(uuid=uuids)
        except CrashDiagramOrientation.DoesNotExist:
            raise Http404

    def patch(self, request, uuids=None):

        getprivatekey = self.get_object(uuids)

        is_active = request.data['is_active']

        if (is_active == 'false'):
            request.data['is_active'] = False
        elif (is_active == 'true'):
            request.data['is_active'] = True
        serializer = DriverCrashDiagramSerializer(getprivatekey, data=request.data)

        if serializer.is_valid():
            serializer.save(is_active=request.data['is_active'])
            return Response([{"Data": serializer.data, "message": "Data updated successfully", "status": True}],
                            status=status.HTTP_201_CREATED)
        return Response([{"Data": serializer.errors, "message": "error", "status": False}],
                        status=status.HTTP_400_BAD_REQUEST)

    def delete(self, request, uuids=None):

        is_active = request.GET.get('is_active')
        if (is_active == 'false'):
            is_active = False
        elif (is_active == 'true'):
            is_active = True
        CrashDiagramOrientation.objects.filter(uuid=uuids).update(is_active=is_active)
        return Response([{"message": "Data deleted successfully", "status": True}])


class DeleteMovementCodeAsPerCrashTypeViewset(APIView):

    def patch(self, request):

        crash_type = request.GET.get("crash_type")

        is_active = request.data['is_active']

        if (is_active == 'false'):
            request.data['is_active'] = False
        elif (is_active == 'true'):
            request.data['is_active'] = True

        CrashDiagramOrientation.objects.filter(crash_type=str(crash_type)).update(is_active=request.data['is_active'])
        # return Response([{ "message": "Data deleted successfully", "status": True}])

        return Response([{"message": "Data deleted successfully", "status": True}])


class DuplicateRecordDetails(APIView):
    def get(self, request, uuid):
        duplicate_record_ids = RecordDuplicate.objects.get(uuid=uuid)
        record_uuid = duplicate_record_ids.record.uuid
        original_record = DriverRecord.objects.get(uuid=record_uuid)
        obj_list = []
        serializer = DriverRecordSerializer(original_record, context={'request': request})
        obj_list.append(serializer.data)
        for i in duplicate_record_ids.potential_duplicates:
            d_obj = DriverRecord.objects.get(uuid=i)
            res = DriverRecordSerializer(d_obj, context={'request': request})
            obj_list.append(res.data)
        return Response(obj_list)


class UseRecord(APIView):
    def post(self, request):
        uuid = request.data.get("uuid")
        recordUUID = request.data.get("recordUUID")
        duplicate_record_ids = RecordDuplicate.objects.filter(record_id=uuid)
        for id in duplicate_record_ids:
            id.resolved = True
            id.save()
            for i in id.potential_duplicates:
                if i not in recordUUID:
                    driver_rec_obj = DriverRecord.objects.get(uuid=i)
                    driver_rec_obj.archived = True
                    driver_rec_obj.save()
            if str(id.record.uuid) not in recordUUID:
                id.record.archived = True
                id.record.save()
        serializer = RecordDuplicateSerializer(duplicate_record_ids, many=True, context={'request': request})
        return Response({'response': 'success'})


########## New API
class DriverGetRecordViewSet(RecordViewSet, mixins.GenerateViewsetQuery):
    """Override base RecordViewSet from grout to provide aggregation and tiler integration
    """
    # permission_classes = (ReadersReadWritersWrite,)
    # filter_class = filters.DriverRecordFilter
    pagination_class = None
    queryset = DriverRecord.objects.all()

    # Filter out everything except details for read-only users
    def get_serializer_class(self):
        # check if parameter details_only is set to true, and if so, use details-only serializer
        requested_details_only = False
        details_only_param = self.request.query_params.get('details_only', None)
        if details_only_param == 'True' or details_only_param == 'true':
            requested_details_only = True

        if (self.request.user):
            if requested_details_only:
                return DetailsReadOnlyRecordNonPublicSerializer
            else:
                return DriverRecordSerializer
        return DetailsReadOnlyRecordSerializer

    def get_queryset(self):
        qs = super(DriverGetRecordViewSet, self).get_queryset()
        if self.get_serializer_class() is DetailsReadOnlyRecordNonPublicSerializer:
            # Add in `created_by` field for user who created the record
            created_by_query = (
                RecordAuditLogEntry.objects.filter(
                    record=OuterRef('pk'),
                    action=RecordAuditLogEntry.ActionTypes.CREATE
                )
                    .annotate(
                    # Fall back to username if the user has been deleted
                    email_or_username=Coalesce('user__email', 'username')
                )
                    .values('email_or_username')
                [:1]
            )
            qs = qs.annotate(created_by=Subquery(created_by_query, output_field=CharField()))
        # Override default model ordering
        return qs.order_by('-occurred_from')

    def get_filtered_queryset(self, request):
        """Return the queryset with the filter backends applied. Handy for aggregations."""
        queryset = self.get_queryset()
        for backend in list(self.filter_backends):
            queryset = backend().filter_queryset(request, queryset, self)
        return queryset

    def add_to_audit_log(self, request, instance, action):
        """Creates a new audit log entry; instance must have an ID"""
        if not instance.pk:
            raise ValueError('Cannot create audit log entries for unsaved model objects')
        if action not in RecordAuditLogEntry.ActionTypes.as_list():
            raise ValueError("{} not one of 'create', 'update', or 'delete'".format(action))
        log = None
        signature = None
        if action == RecordAuditLogEntry.ActionTypes.CREATE:
            log = serializers.serialize(
                'json',
                [
                    DriverRecord.objects.get(pk=instance.pk),
                    Record.objects.get(pk=instance.record_ptr_id)
                ]
            )
            # signature = hashlib.md5(log).hexdigest()
            signature = hashlib.sha256(log.encode('utf-8')).hexdigest()
        RecordAuditLogEntry.objects.create(
            user=request.user,
            username=request.user.username,
            record=instance,
            record_uuid=str(instance.pk),
            action=action,
            log=log,
            signature=signature
        )

    @transaction.atomic
    def perform_create(self, serializer):
        instance = serializer.save()
        self.add_to_audit_log(self.request, instance, RecordAuditLogEntry.ActionTypes.CREATE)

    @transaction.atomic
    def perform_update(self, serializer):
        instance = serializer.save()
        self.add_to_audit_log(self.request, instance, RecordAuditLogEntry.ActionTypes.UPDATE)

    @transaction.atomic
    def perform_destroy(self, instance):
        self.add_to_audit_log(self.request, instance, RecordAuditLogEntry.ActionTypes.DELETE)
        instance.delete()

        # Views

    def list(self, request, *args, **kwargs):
        # Don't generate a tile key unless the user specifically requests it, to avoid
        # filling up the Redis cache with queries that will never be viewed as tiles
        if ('tilekey' in request.query_params and
                request.query_params['tilekey'] in ['True', 'true']):
            response = Response(dict())
            query_sql = self.generate_query_sql(request)
            tile_token = str(uuid.uuid4())
            self._cache_tile_sql(tile_token, query_sql)
            response.data['tilekey'] = tile_token
        else:
            response = super(DriverGetRecordViewSet, self).list(self, request, *args, **kwargs)
        ############### For Year Wise ################
        if "flag" in request.query_params and int(request.query_params['flag']) == 1:
            """
            FOR ANUAL RECORDS
            """

            list_of_dca_codes = []
            response_data = response.data
            for i in response_data:
                data = i["data"]
                if "driverCrashDiagram" in data:
                    if "Movement Code" in data["driverCrashDiagram"]:
                        dca_code = data["driverCrashDiagram"]["Movement Code"]
                        if dca_code not in list_of_dca_codes:
                            if dca_code:
                                list_of_dca_codes.append(dca_code)

            final_list = []
            dca_code = "DCA_code"
            for i in list_of_dca_codes:
                # single_list = []
                final_dict = {dca_code: "", "no_of_crashesh_year": {},
                              "direction_of_other_vehicle": {},
                              "type_of_road_users": {}, "surface": {},
                              "light_condition": {}, "day_of_week": {}}
                years_list = []
                years_dict = {}
                vehicle_type_list = []
                vehicle_type_dict = {}
                surface_list = []
                surface_dict = {}
                light_condition_list = []
                light_condition_dict = {}
                days_list = []
                days_dict = {}

                to_north = 0
                to_east = 0
                to_west = 0
                to_south = 0

                for res_data in response_data:
                    data = res_data["data"]
                    if "driverCrashDiagram" in data:
                        if "Movement Code" in data["driverCrashDiagram"]:
                            if data["driverCrashDiagram"]["Movement Code"] == i:
                                if "driverVehicle" in data:
                                    for driver_item in data["driverVehicle"]:
                                        if 'Surface' in driver_item:
                                            surface_list.append(driver_item['Surface'])

                                        if "Vehicle type" in driver_item:
                                            vehicle_type = driver_item["Vehicle type"]
                                            vehicle_type_list.append(vehicle_type)
                                        if "Direction" in driver_item:
                                            if "To East" in driver_item["Direction"]:
                                                to_east = to_east + 1
                                            if "To West" in driver_item["Direction"]:
                                                to_west = to_west + 1
                                            if "To North" in driver_item["Direction"]:
                                                to_north = to_north + 1
                                            if "To South" in driver_item["Direction"]:
                                                to_south = to_south + 1

                                if "driverIncidentDetails" in data:
                                    if "Road Condition" in data["driverIncidentDetails"]:
                                        pass
                                        # surface_list.append(data["driverIncidentDetails"]["Road Condition"])
                                if "light" in res_data:
                                    if res_data["light"]:
                                        light_condition_list.append(res_data["light"])
                                    year = res_data["occurred_from"].split("-")[0]
                                years_list.append(year)
                                occured_date = res_data["occurred_from"].split("T")
                                year, month, day = (int(i) for i in occured_date[0].split('-'))
                                dayNumber = calendar.weekday(year, month, day)
                                days = ["Monday", "Tuesday", "Wednesday", "Thursday",
                                        "Friday", "Saturday", "Sunday"]
                                day_of_week = days[dayNumber]
                                days_list.append(day_of_week)
                for year_item in years_list:
                    years_dict[year_item] = years_list.count(year_item)
                years_dict["Total"] = sum(years_dict.values())
                for vehicle_item in vehicle_type_list:
                    vehicle_type_dict[vehicle_item] = vehicle_type_list.count(vehicle_item)
                for surface_item in surface_list:
                    surface_dict[surface_item] = surface_list.count(surface_item)
                for light_item in light_condition_list:
                    light_condition_dict[light_item] = light_condition_list.count(light_item)
                for day_item in days_list:
                    days_dict[day_item] = days_list.count(day_item)

                days_dict["weekday"] = sum(days_dict.values())
                final_dict["no_of_crashesh_year"] = years_dict
                final_dict["type_of_road_users"] = vehicle_type_dict
                final_dict["surface"] = surface_dict
                # final_dict["Road Condition"] = surface_dict
                final_dict["light_condition"] = light_condition_dict

                ################# FOR ADD Most common day ############

                if days_dict:
                    newdaysdict = copy.deepcopy(days_dict)
                    if 'weekday' in newdaysdict:
                        weekdayval = newdaysdict.pop("weekday")

                    maxday = max(newdaysdict.items(), key=operator.itemgetter(1))

                    for days in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]:
                        if days in newdaysdict:
                            newdaysdict.pop(days)

                    if str(weekdayval):
                        newdaysdict["weekday"] = weekdayval
                    newdaysdict["Most common day (if any)"] = maxday[0]
                    final_dict["day_of_week"] = newdaysdict
                else:
                    final_dict["day_of_week"] = days_dict
                ################# FOR ADD Most common day ############

                final_dict[dca_code] = i
                direction_dict = {}
                if to_north != 0:
                    direction_dict["To North"] = str(to_north)
                if to_east != 0:
                    direction_dict["To East"] = str(to_east)
                if to_west != 0:
                    direction_dict["To West"] = str(to_west)
                if to_south != 0:
                    direction_dict["To South"] = str(to_south)

                final_dict["direction_of_other_vehicle"] = direction_dict

                final_list.append(final_dict)
                ################# Add All keys in 0th indexing #############
                years_keys = []
                road_users_keys = []
                surface_keys = []
                light_condition_keys = []
                day_of_week_keys = []
                direction_keys = []
                for item in final_list:
                    for i in item["direction_of_other_vehicle"]:
                        direction_keys.append(i)
                    for i in item["no_of_crashesh_year"].keys():
                        years_keys.append(i)
                    for i in item["type_of_road_users"].keys():
                        road_users_keys.append(i)
                    for i in item["surface"].keys():
                        surface_keys.append(i)
                    for i in item["light_condition"].keys():
                        light_condition_keys.append(i)
                    for i in item["day_of_week"].keys():
                        day_of_week_keys.append(i)

                for i in direction_keys:
                    if i not in final_list[0]["direction_of_other_vehicle"]:
                        final_list[0]["direction_of_other_vehicle"][i] = ""
                for i in years_keys:
                    if i not in final_list[0]["no_of_crashesh_year"]:
                        final_list[0]["no_of_crashesh_year"][i] = None
                for i in road_users_keys:
                    if i not in final_list[0]["type_of_road_users"]:
                        final_list[0]["type_of_road_users"][i] = None
                for i in surface_keys:
                    if i not in final_list[0]["surface"]:
                        final_list[0]["surface"][i] = None
                for i in light_condition_keys:
                    if i not in final_list[0]["light_condition"]:
                        final_list[0]["light_condition"][i] = None
                for i in day_of_week_keys:
                    if i not in final_list[0]["day_of_week"]:
                        final_list[0]["day_of_week"][i] = None

            ################# End of Add All keys in 0th indexing #############

            ####################### FOR TOTALS #######################

            if not final_list:
                return Response(final_list)

            if "day_of_week" in final_list[0]:
                if "Saturday" in final_list[0]["day_of_week"]:
                    final_list[0]["day_of_week"]["a-Saturday"] = final_list[0]["day_of_week"].pop("Saturday")
                if "Sunday" in final_list[0]["day_of_week"]:
                    final_list[0]["day_of_week"]["b-Sunday"] = final_list[0]["day_of_week"].pop("Sunday")
                if "weekday" in final_list[0]["day_of_week"]:
                    final_list[0]["day_of_week"]["c-weekday"] = final_list[0]["day_of_week"].pop("weekday")
                if "Most common day (if any)" in final_list[0]["day_of_week"]:
                    final_list[0]["day_of_week"]["d-Most common day (if any)"] = final_list[0]["day_of_week"].pop(
                        "Most common day (if any)")

            final_list[0]["day_of_week"] = dict(sorted(final_list[0]["day_of_week"].items()))

            if "day_of_week" in final_list[0]:
                for key in list(final_list[0]["day_of_week"]):
                    final_list[0]["day_of_week"][key.split("-")[1]] = final_list[0]["day_of_week"].pop(key)

            if dca_code in final_list[0]:
                final_list[0]["a-" + dca_code] = final_list[0].pop(dca_code)
            if "no_of_crashesh_year" in final_list[0]:
                final_list[0]["b-no_of_crashesh_year"] = final_list[0].pop("no_of_crashesh_year")
            if "direction_of_other_vehicle" in final_list[0]:
                final_list[0]["c-direction_of_other_vehicle"] = final_list[0].pop("direction_of_other_vehicle")
            if "type_of_road_users" in final_list[0]:
                final_list[0]["d-type_of_road_users"] = final_list[0].pop("type_of_road_users")
            if "surface" in final_list[0]:
                final_list[0]["e-surface"] = final_list[0].pop("surface")
            if "light_condition" in final_list[0]:
                final_list[0]["f-light_condition"] = final_list[0].pop("light_condition")
            if "day_of_week" in final_list[0]:
                final_list[0]["g-day_of_week"] = final_list[0].pop("day_of_week")

            final_list[0] = dict(sorted(final_list[0].items()))

            if "a-" + dca_code in final_list[0]:
                final_list[0][dca_code] = final_list[0].pop("a-" + dca_code)
            if "b-no_of_crashesh_year" in final_list[0]:
                final_list[0]["no_of_crashesh_year"] = final_list[0].pop("b-no_of_crashesh_year")
            if "c-direction_of_other_vehicle" in final_list[0]:
                final_list[0]["direction_of_other_vehicle"] = final_list[0].pop("c-direction_of_other_vehicle")
            if "d-type_of_road_users" in final_list[0]:
                final_list[0]["type_of_road_users"] = final_list[0].pop("d-type_of_road_users")
            if "e-surface" in final_list[0]:
                final_list[0]["surface"] = final_list[0].pop("e-surface")
            if "f-light_condition" in final_list[0]:
                final_list[0]["light_condition"] = final_list[0].pop("f-light_condition")
            if "g-day_of_week" in final_list[0]:
                final_list[0]["day_of_week"] = final_list[0].pop("g-day_of_week")

            new_final_list = copy.deepcopy(final_list)

            first_index = copy.deepcopy(new_final_list[0])

            if dca_code in first_index:
                first_index.pop(dca_code)
            if "key_direction" in first_index:
                first_index.pop("key_direction")
            if "Most common day (if any)" in first_index["day_of_week"]:
                first_index["day_of_week"].pop("Most common day (if any)")

            new_keys_dict = {}
            for singleidict in first_index:
                new_keys_dict[str(singleidict)] = {}
                for keys in first_index[singleidict].keys():
                    new_keys_dict[str(singleidict)][keys] = []

            for dictitem in new_final_list:
                if dca_code in dictitem:
                    dictitem.pop(dca_code)
                if "key_direction" in dictitem:
                    dictitem.pop("key_direction")
                if "Most common day (if any)" in dictitem["day_of_week"]:
                    dictitem["day_of_week"].pop("Most common day (if any)")

                for singleitem in dictitem:
                    for itemindex in new_keys_dict:
                        if singleitem == itemindex:
                            if dictitem[singleitem]:
                                for nested_value in dictitem[singleitem]:
                                    value = dictitem[singleitem][nested_value]
                                    if value not in ["", None]:
                                        new_keys_dict[singleitem][nested_value].append(int(value))

            totals = {}
            for i in list(new_keys_dict):
                if new_keys_dict[i]:
                    totals[i] = {}
                    for keys in new_keys_dict[i].keys():
                        try:
                            totals[i][keys] = sum(new_keys_dict[i][keys])
                        except:
                            pass
                else:
                    totals[i] = new_keys_dict[i]

            totals[dca_code] = "Total"
            final_list.append(totals)
            ####################### FOR TOTALS #######################
            return Response(final_list)

        elif "flag" in request.query_params and int(request.query_params['flag']) == 0:
            """
            FOR INDIVISUAL RECORDS
            """
            datalist = []
            datacount = 0
            response_data = response.data
            if not response_data:
                return Response(datalist)
            datakeys = response_data[0]
            removekeys = ["uuid", "geom", "created", "modified", "occurred_to"]

            keys_list = [i for i in datakeys if i not in removekeys]

            # for i in datakeys:
            #     if i not in removekeys:
            #         keys_list.append(i)


            for dataitem in response_data:
                datadict = {}
                datacount += 1
                datadict["Crash number"] = datacount
                for keyitem in keys_list:
                    if keyitem == "data":
                        for nesteditem in dataitem[keyitem]:
                            for itemkey in dataitem[keyitem][nesteditem]:
                                if itemkey in ["_localId", "Description"]:
                                    pass
                                else:
                                    if nesteditem == "driverPhoto":
                                        pass
                                    else:
                                        if nesteditem == "driverVehicle":
                                            if '_localId' in dataitem[keyitem][nesteditem][0]:
                                                dataitem[keyitem][nesteditem][0].pop("_localId")
                                            demodict = {}
                                            for i in dataitem[keyitem][nesteditem][0].keys():
                                                demodict[i] = []
                                            obj_count = 0
                                            direction_count = 0
                                            for i in dataitem[keyitem][nesteditem]:
                                                obj_count += 1
                                                direction_count += 1
                                                if ("Vehicle type" or "Direction") in i:
                                                    if "Vehicle type" in i:
                                                        try:
                                                            datadict["Object" + " " + str(obj_count)] = i[
                                                                "Vehicle type"]
                                                        except:
                                                            datadict["Object" + " " + str(obj_count)] = None
                                                    else:
                                                        datadict["Object" + " " + str(obj_count)] = None

                                                    if "Direction" in i:
                                                        try:
                                                            datadict["Direction" + " " + str(direction_count)] = i[
                                                                "Direction"]
                                                        except:
                                                            datadict["Direction" + " " + str(direction_count)] = None
                                                    else:
                                                        datadict["Direction" + " " + str(direction_count)] = None

                                                if "Surface" in i:
                                                    datadict["Road Condition"] = i["Surface"]

                                                for nestedkey in demodict:
                                                    if i[nestedkey]:
                                                        demodict[nestedkey].append(i[nestedkey])
                                            for key, value in demodict.items():
                                                datadict[key] = value
                                        else:

                                            if type(dataitem[keyitem][nesteditem]) == list:
                                                for listitem in dataitem[keyitem][nesteditem]:
                                                    if "_localId" in listitem:
                                                        listitem.pop("_localId")
                                                    else:
                                                        for li in listitem:
                                                            datadict[str(li)] = listitem[li]
                                            else:
                                                valuedata = dataitem[keyitem][nesteditem][str(itemkey)]
                                                if 'data:image' in valuedata:
                                                    pass
                                                else:
                                                    if str(itemkey) == "Movement Code":
                                                        datadict["DCA code"] = valuedata
                                                    else:
                                                        datadict[str(itemkey)] = valuedata
                    else:
                        if keyitem == "occurred_from":
                            occured_date = dataitem["occurred_from"].split("T")
                            year, month, day = (int(i) for i in occured_date[0].split('-'))
                            dayNumber = calendar.weekday(year, month, day)
                            days = ["Monday", "Tuesday", "Wednesday", "Thursday",
                                    "Friday", "Saturday", "Sunday"]
                            day_of_week = days[dayNumber]
                            if len(str(day)) == 1:
                                date = str(0) + str(day)
                            else:
                                date = str(day)
                            if len(str(month)) == 1:
                                month = str(0) + str(month)
                            else:
                                month = str(month)
                            day_month = date + "-" + month
                            time_of_day = occured_date[1].split(":")[0] + ":" + occured_date[1].split(":")[1]
                            datadict["Day of week"] = day_of_week
                            datadict["Date: day-month"] = day_month
                            datadict["Date: year"] = year
                            datadict["Time of day"] = time_of_day
                        else:
                            valuedata = dataitem[str(keyitem)]
                            datadict[str(keyitem)] = valuedata

                datalist.append(datadict)

            keys_list = ["Crash number", "Day of week", "Date: day-month", "Date: year",
                         "Time of day", "Severity", "light", "Road Condition", "DCA code", "Object", "Direction"]
            for singlejson in datalist:
                for i in list(singlejson):
                    if i == "Direction":
                        singlejson.pop(i)
                    elif i.startswith("Object"):
                        pass
                    elif i.startswith("Direction"):
                        pass
                    elif i in keys_list:
                        pass
                    else:
                        singlejson.pop(i)

            maxkeys = []
            for i in datalist:
                maxkeys.append(len(i))
            indexno = maxkeys.index(max(maxkeys))
            for i in datalist[indexno].keys():
                if i not in datalist[0]:
                    datalist[0][i] = None

            # all_years = [i['Date: year'] for i in datalist]
            # unique_years = [i for i in set(all_years)]
            # for i in unique_years:
            #     for item in datalist:
            #         if item['Date: year'] == i:
            #
            # keys_list = ["Crash number", "Day of week", "Date: day-month", "Date: year",
            #              "Time of day", "Severity", "light", "Road Condition", "DCA code"]
            all_keys = datalist[0].keys()

            if "Crash number" in datalist[0]:
                datalist[0]["a--Crash number"] = datalist[0].pop("Crash number")
            if "Date: day-month" in datalist[0]:
                datalist[0]["b--Date: day-month"] = datalist[0].pop("Date: day-month")
            if "Date: year" in datalist[0]:
                datalist[0]["c--Date: year"] = datalist[0].pop("Date: year")
            if "Day of week" in datalist[0]:
                datalist[0]["d--Day of week"] = datalist[0].pop("Day of week")
            if "Time of day" in datalist[0]:
                datalist[0]["e--Time of day"] = datalist[0].pop("Time of day")
            if "Severity" in datalist[0]:
                datalist[0]["f--Severity"] = datalist[0].pop("Severity")
            if "light" in datalist[0]:
                datalist[0]["g--light"] = datalist[0].pop("light")
            if "Road Condition" in datalist[0]:
                datalist[0]["h--Road Condition"] = datalist[0].pop("Road Condition")
            if "DCA code" in datalist[0]:
                datalist[0]["i--DCA code"] = datalist[0].pop("DCA code")

            objects_list = []
            directions_list = []

            for i in all_keys:
                if i.startswith("Object"):
                    objects_list.append(i)
                if i.startswith("Direction"):
                    directions_list.append(i)

            objects_list.sort()
            directions_list.sort()

            keycount = "i"

            for i in objects_list:
                keycount = chr(ord(keycount) + 1)
                if i in datalist[0]:
                    datalist[0][str(keycount) + "--" + i] = datalist[0].pop(i)

            for i in directions_list:
                keycount = chr(ord(keycount) + 1)
                if i in datalist[0]:
                    datalist[0][str(keycount) + "--" + i] = datalist[0].pop(i)

            datalist[0] = dict(sorted(datalist[0].items()))
            new_dict_first_index = {}
            for i in list(datalist[0]):
                new_dict_first_index[i.split("--")[1]] = datalist[0].pop(i)
            datalist[0] = new_dict_first_index

            return Response(datalist)

        elif "flag" in request.query_params and int(request.query_params['flag']) == 2:
            list_of_dca_codes = []
            response_data = response.data
            for i in response_data:
                data = i["data"]
                if "driverCrashDiagram" in data:
                    if "Movement Code" in data["driverCrashDiagram"]:
                        dca_code = data["driverCrashDiagram"]["Movement Code"]
                        if dca_code not in list_of_dca_codes:
                            if dca_code:
                                list_of_dca_codes.append(dca_code)
            datalist = []
            for i in list_of_dca_codes:
                for res_data in response_data:
                    data = res_data["data"]
                    if "driverCrashDiagram" in data:
                        if "Movement Code" in data["driverCrashDiagram"]:
                            if data["driverCrashDiagram"]["Movement Code"] == i:
                                datalist.append(res_data)
            return Response(datalist)

    def _cache_tile_sql(self, token, sql):
        """Stores a sql string in the common cache so it can be retrieved by Windshaft later"""
        # We need to use a raw Redis connection because the Django cache backend
        # transforms the keys and values before storing them. If the cached data
        # were being read by Django, this transformation would be reversed, but
        # since the stored sql will be parsed by Windshaft / Postgres, we need
        # to store the data exactly as it is.
        redis_conn = get_redis_connection('default')
        # redis_conn.set(token, sql.encode('utf-8'))
        redis_conn.set(str(token), sql)

    @action(methods=['get'], detail=False)
    def stepwise(self, request):
        """Return an aggregation counts the occurrence of events per week (per year) between
        two bounding datetimes
        e.g. [{"week":35,"count":13,"year":2015},{"week":43,"count":1,"year":2015}]
        """
        # We'll need to have minimum and maximum dates specified to properly construct our SQL
        try:
            start_date = parse_date(request.query_params['occurred_min'])
            end_date = parse_date(request.query_params['occurred_max'])
        except KeyError:
            raise ParseError("occurred_min and occurred_max must both be provided")
        except ValueError:
            raise ParseError("occurred_min and occurred_max must both be valid dates")

        # The min year can't be after or more than 2000 years before the max year
        year_distance = end_date.year - start_date.year
        if year_distance < 0:
            raise ParseError("occurred_min must be an earlier date than occurred_max")
        if year_distance > 2000:
            raise ParseError("occurred_min and occurred_max must be within 2000 years of one another")

        queryset = self.get_filtered_queryset(request)

        # Build SQL `case` statement to annotate with the year
        isoyear_case = Case(*[When(occurred_from__isoyear=year, then=Value(year))
                              for year in range(start_date.year, end_date.year + 1)],
                            output_field=IntegerField())
        # Build SQL `case` statement to annotate with the day of week
        week_case = Case(*[When(occurred_from__week=week, then=Value(week))
                           for week in xrange(1, 54)],
                         output_field=IntegerField())

        annotated_recs = queryset.annotate(year=isoyear_case).annotate(week=week_case)

        # Voodoo to perform aggregations over `week` and `year` combinations
        counted = (annotated_recs.values('week', 'year')
                   .order_by('week', 'year')
                   .annotate(count=Count('week')))

        return Response(counted)

    @action(methods=['get'], detail=False)
    def toddow(self, request):
        """ Return aggregations which nicely format the counts for time of day and day of week
        e.g. [{"count":1,"dow":6,"tod":1},{"count":1,"dow":3,"tod":3}]
        """
        queryset = self.get_filtered_queryset(request)
        counted = build_toddow(queryset)
        return Response(counted)

    @action(methods=['get'], detail=False)
    def recent_counts(self, request):
        """ Return the recent record counts for 30, 90, 365 days """
        now = datetime.datetime.now(tz=pytz.timezone(settings.TIME_ZONE))
        qs = self.get_filtered_queryset(request).filter(occurred_from__lte=now)
        durations = {
            'month': 30,
            'quarter': 90,
            'year': 365,
        }

        counts = {label: qs.filter(occurred_from__gte=(now - datetime.timedelta(days=days))).count()
                  for label, days in durations.items()}
        return Response(counts)

    @action(methods=['get'], detail=False)
    def costs(self, request):
        """Return the costs for a set of records of a certain type

        This endpoint requires the record_type query parameter. All other query parameters will be
        used to filter the queryset before calculating costs.

        There must be a RecordCostConfig associated with the RecordType passed, otherwise a 404
        will be returned. If there are multiple RecordCostConfigs associated with a RecordType, the
        most recently created one will be used.

        Uses the most recent schema for the RecordType; if this doesn't match the fields in the
        RecordCostConfig associated with the RecordType, an exception may be raised.

        Returns a response of the form:
        {
            total: X,
            subtotals: {
                enum_choice1: A,
                enum_choice2: B,
                ...
            }
        }
        """
        record_type_id = request.query_params.get('record_type', None)
        if not record_type_id:
            raise ParseError(detail="The 'record_type' parameter is required")
        cost_config = (RecordCostConfig.objects.filter(record_type_id=record_type_id)
                       .order_by('-created').first())
        if not cost_config:
            return Response({'record_type': 'No cost configuration found for this record type.'},
                            status=status.HTTP_404_NOT_FOUND)
        schema = RecordType.objects.get(pk=record_type_id).get_current_schema()
        path = cost_config.path
        multiple = self._is_multiple(schema, path)
        choices = self._get_schema_enum_choices(schema, path)
        # `choices` may include user-entered data; to prevent users from entering column names
        # that conflict with existing Record fields, we're going to use each choice's index as an
        # alias instead.
        choice_indices = {str(idx): choice for idx, choice in enumerate(choices)}
        counts_queryset = self.get_filtered_queryset(request)
        for idx, choice in choice_indices.items():
            filter_rule = self._make_djsonb_containment_filter(path, choice, multiple)
            # We want a column for each enum choice with a binary 1/0 indication of whether the row
            # in question has that enum choice. This is to support checkbox fields which can have
            # more than one selection from the enum per field. Then we're going to sum those to get
            # aggregate counts for each enum choice.
            choice_case = Case(When(data__jsonb=filter_rule, then=Value(1)), default=Value(0),
                               output_field=IntegerField())
            annotate_params = dict()
            annotate_params[idx] = choice_case
            counts_queryset = counts_queryset.annotate(**annotate_params)
        output_data = {'prefix': cost_config.cost_prefix, 'suffix': cost_config.cost_suffix}
        if not counts_queryset:  # Short-circuit if no events at all
            output_data.update({"total": 0, "subtotals": {choice: 0 for choice in choices},
                                "outdated_cost_config": False})
            return Response(output_data)
        # Do the summation
        sum_ops = [Sum(key) for key in choice_indices.keys()]
        sum_qs = counts_queryset.values(*choice_indices.keys()).aggregate(*sum_ops)
        # sum_qs will now look something like this: {'0__sum': 20, '1__sum': 45, ...}
        # so now we need to slot in the corresponding label from `choices` by pulling the
        # corresponding value out of choices_indices.
        sums = {}
        for key, choice_sum in sum_qs.items():
            index = key.split('_')[0]
            choice = choice_indices[index]
            sums[choice] = choice_sum
        # Multiply sums by per-incident costs to get subtotal costs broken out by type
        subtotals = dict()
        # This is going to be extremely easy for users to break if they update a schema without
        # updating the corresponding cost configuration; if things are out of sync, degrade
        # gracefully by providing zeroes for keys that don't match and set a flag so that the
        # front-end can alert users if needed.
        found_missing_choices = False
        for key, value in sums.items():
            try:
                total_subs = value * int(cost_config.enum_costs[key])
                subtotals[key] = f"{total_subs:,d}"
            except KeyError:
                logger.warning('Schema and RecordCostConfig out of sync; %s missing from cost config',
                               key)
                found_missing_choices = True
                subtotals[key] = 0
        total = sum(subtotals.values())
        # Return breakdown costs and sum
        output_data.update({'total': f"{total:,d}", 'subtotals': subtotals,
                            'outdated_cost_config': found_missing_choices})
        return Response(output_data)

    @action(methods=['get'], detail=False)
    def crosstabs(self, request):
        """Returns a columnar aggregation of event totals; this is essentially a generalized ToDDoW

        Requires the following query parameters:
        - Exactly one row specification parameter chosen from:
            - row_period_type: A time period to use as rows; valid choices are:
                               {'hour', 'day', 'week_day', 'week', 'month', 'year'}
                               The value 'day' signifies day-of-month
            - row_boundary_id: Id of a Boundary whose BoundaryPolygons should be used as rows
            - row_choices_path: Path components to a schema property whose choices should be used
                                as rows, separated by commas
                                e.g. &row_choices_path=incidentDetails,properties,Collision%20type
                                Note that ONLY properties which have an 'enum' key are valid here.
        - Exactly one column specification parameter chosen from:
            - col_period_type
            - col_boundary_id
            - col_choices_path
            As you might expect, these operate identically to the row_* parameters above, but the
            results are used as columns instead.
        - record_type: The UUID of the record type which should be aggregated
        - calendar: the calendar to use for the report to (Ex: 'gregorian' or 'ummalqura')

        Allows the following query parameters:
        - aggregation_boundary: Id of a Boundary; separate tables will be generated for each
                                BoundaryPolygon associated with the Boundary.
        - all other filter params accepted by the list endpoint; these will filter the set of
            records before any aggregation is applied. This may result in some rows / columns /
            tables having zero records.

        Note that the tables are sparse: rows will only appear in 'data' and 'row_totals',
        and columns will only appear in rows, if there are values returned by the query.
        'row_labels' and 'col_labels', however, are complete and in order.

        Response format:
        {
            "row_labels": [
                { "key": "row_key1", "label": "row_label1"},
                ...
            ],
            "col_labels": [
                { "key": "col_key1", "label": "col_label1"},
                ...
            ],
            "table_labels": {
                "table_key1": "table_label1",
                // This will be empty if aggregation_boundary is not provided
            },
            "tables": [
                {
                    "tablekey": "table_key1",
                    "data": {
                        {
                            "row_key1": {
                                "col_key1": N,
                                "col_key3": N,
                            },
                        },
                        ...
                    },
                    "row_totals": {
                        {
                            "row_key1": N,
                            "row_key3": N,
                    }
                },
                ...
            ]
        }
        """
        valid_row_params = set(['row_period_type', 'row_boundary_id', 'row_choices_path'])
        valid_col_params = set(['col_period_type', 'col_boundary_id', 'col_choices_path'])
        # Validate there's exactly one row_* and one col_* parameter
        row_params = set(request.query_params) & valid_row_params
        col_params = set(request.query_params) & valid_col_params
        if len(row_params) != 1 or len(col_params) != 1:
            raise ParseError(detail='Exactly one col_* and row_* parameter required; options are {}'
                             .format(list(valid_row_params | valid_col_params)))

        # Get queryset, pre-filtered based on other params
        queryset = self.get_filtered_queryset(request)
        # Pass parameters to case-statement generators
        row_param = row_params.pop()  # Guaranteed to be just one at this point
        col_param = col_params.pop()
        row_multi, row_labels, annotated_qs = self._query_param_to_annotated_tuple(
            row_param, request, queryset, 'row')
        col_multi, col_labels, annotated_qs = self._query_param_to_annotated_tuple(
            col_param, request, annotated_qs, 'col')

        # If aggregation_boundary_id exists, grab the associated BoundaryPolygons.
        tables_boundary = request.query_params.get('aggregation_boundary', None)
        if tables_boundary:
            boundaries = BoundaryPolygon.objects.filter(boundary=tables_boundary)
        else:
            boundaries = None

        # Assemble crosstabs either once or multiple times if there are boundaries
        response = dict(tables=[], table_labels=dict(), row_labels=row_labels,
                        col_labels=col_labels)
        if boundaries:
            # Add table labels
            parent = Boundary.objects.get(pk=tables_boundary)
            response['table_labels'] = {str(poly.pk): poly.data[parent.display_field]
                                        for poly in boundaries}
            # Filter by polygon for counting
            for poly in boundaries:
                table = self._fill_table(
                    annotated_qs.filter(geom__within=poly.geom),
                    row_multi, row_labels, col_multi, col_labels)
                table['tablekey'] = poly.pk
                response['tables'].append(table)
        else:
            response['tables'].append(self._fill_table(
                annotated_qs, row_multi, row_labels, col_multi, col_labels))

        return Response(response)

    def _fill_table(self, annotated_qs, row_multi, row_labels, col_multi, col_labels):
        """ Fill a nested dictionary with the counts and compute row totals. """

        # The data being returned is a nested dictionary: row label -> col labels = integer count
        data = defaultdict(lambda: defaultdict(int))
        if not row_multi and not col_multi:
            # Not in multi-mode: sum rows/columns by a simple count annotation.
            # This is the normal case.
            # Note: order_by is necessary here -- it's what triggers django to do a group by.

            for value in (annotated_qs.values('row', 'col')
                    .order_by('row', 'col')):
                if value['row'] is not None and value['col'] is not None:
                    data[str(value['row'])][str(value['col'])] = value['count']
        elif row_multi and col_multi:
            # The row and column are in multi-mode, iterate to build up counts.
            # This is a very rare case, since creating a report between two 'multiple' items
            # doesn't seem very useful, at least with the current set of data. We may even end
            # up restricting this via the front-end. But until then, it's been implemented
            # here, and it works, but is on the slow side, since it needs to manually aggregate.
            for record in annotated_qs:
                rd = record.__dict__
                row_ids = [
                    str(label['key'])
                    for label in row_labels
                    if rd['row_{}'.format(label['key'])] > 0
                ]
                col_ids = [
                    str(label['key'])
                    for label in col_labels
                    if rd['col_{}'.format(label['key'])] > 0
                ]
                # Each object has row_* and col_* fields, where a value > 0 indicates presence.
                # Increment the counter for each combination.
                for row_id in row_ids:
                    for col_id in col_ids:
                        data[row_id][col_id] += 1
        else:
            # Either the row or column is a 'multiple' item, but not both.
            # This is a relatively common case and is still very fast since the heavy-lifting
            # is all done within the db.
            if row_multi:
                multi_labels = row_labels
                single_label = 'col'
                multi_prefix = 'row'
            else:
                multi_labels = col_labels
                single_label = 'row'
                multi_prefix = 'col'

            multi_labels = [
                '{}_{}'.format(multi_prefix, str(label['key']))
                for label in multi_labels
            ]

            # Perform a sum on each of the 'multi' columns, storing the data in a sum_* field
            annotated_qs = (
                annotated_qs.values(single_label, *multi_labels)
                    .order_by()
                    .annotate(**{'sum_{}'.format(label): Sum(label) for label in multi_labels}))

            # Iterate over each object and accumulate each sum in the proper dictionary position.
            # Each object either has a 'row' and several 'col_*'s or a 'col' and several 'row_*'s.
            # Get the combinations accordingly and accumulate the appropriate stored value.
            for rd in annotated_qs:
                for multi_label in multi_labels:
                    sum_val = rd['sum_{}'.format(multi_label)]
                    rd_row = rd['row'] if 'row' in rd else 'None'
                    rd_col = rd['col'] if 'col' in rd else 'None'

                    if row_multi:
                        data[str(multi_label[4:])][str(rd_col)] += sum_val
                    else:
                        data[str(rd_row)][str(multi_label[4:])] += sum_val

        row_totals = {row: sum(cols.values()) for (row, cols) in data.items()}
        return {'data': data, 'row_totals': row_totals}

    def _get_annotated_tuple(self, queryset, annotation_id, case, labels):
        """Helper wrapper for annotating a queryset with a case statement

        Args:
          queryset (QuerySet): The input queryset
          annotation_id (String): 'row' or 'col'
          case (Case): The generated Case statement
          labels (dict<Case, String>): dict mapping Case values to labels

        Returns:
            A 3-tuple of:
              - boolean which specifies whether this is a 'multiple' query (always False)
              - dict mapping Case values to labels
              - the newly-annotated queryset
        """
        kwargs = {}
        kwargs[annotation_id] = case
        annotated_qs = queryset.annotate(**kwargs)
        return (False, labels, annotated_qs)

    def _query_param_to_annotated_tuple(self, param, request, queryset, annotation_id):

        """Wrapper to handle getting the params for each case generator because we do it twice. TODO....."""
        try:
            record_type_id = request.query_params['record_type']
        except KeyError:
            raise ParseError(detail="The 'record_type' parameter is required")

        if param.endswith('period_type'):
            query_calendar = request.query_params.get('calendar')
            if (query_calendar == 'gregorian'):
                return self._get_annotated_tuple(
                    queryset, annotation_id,
                    *self._make_gregorian_period_case(
                        request.query_params[param], request, queryset))
            elif (query_calendar == 'ummalqura'):
                return self._get_annotated_tuple(
                    queryset, annotation_id,
                    *self._make_ummalqura_period_case(
                        request.query_params[param], request, queryset))
        elif param.endswith('boundary_id'):
            return self._get_annotated_tuple(
                queryset, annotation_id, *self._make_boundary_case(request.query_params[param]))
        else:  # 'choices_path'; ensured by parent function
            schema = RecordType.objects.get(pk=record_type_id).get_current_schema()
            path = request.query_params[param].split(',')
            multiple = self._is_multiple(schema, path)

            if (not multiple):
                return self._get_annotated_tuple(
                    queryset, annotation_id,
                    *self._make_choices_case(schema, path))
            else:
                # A 'multiple' related object must be annotated differently,
                # since it may fall into multiple different categories.
                return self._get_multiple_choices_annotated_tuple(
                    queryset, annotation_id, schema, path)

    def _get_day_label(self, week_day_index):
        """Constructs a day translation label string given a week day index

        Args:
            week_day_index (int): Django `week_day` property (1-indexed, starting with Sunday)

        Returns:
            A string representing the day translation label
        """
        # week_day is 1-indexed and starts with Sunday, whereas day_name
        # is 0-indexed and starts with Monday, so we need to map indices as follows:
        # 1,2,3,4,5,6,7 -> 6,0,1,2,3,4,5 for Sunday through Saturday
        return 'DAY.{}'.format(
            calendar.day_name[6 if week_day_index == 1 else week_day_index - 2].upper()
        )

    # def _make_gregorian_period_case(self, period_type, request, queryset):
    #     """Constructs a Django Case statement for a certain type of period.

    #     Args:
    #         period_type (string): one of the valid aggegation type keys, either periodic (e.g.
    #             'day_of_week', 'month_of_year') or sequential (e.g. 'day', 'month', 'year')
    #         request (Request): the request, from which max and min date will be read if needed
    #         queryset (QuerySet): filtered queryset to use for getting date range if it's needed
    #             and the request is missing a max and/or min date
    #     Returns:
    #         (Case, labels), where Case is a Django Case object giving the period in which each
    #         record's occurred_from falls, and labels is a dict mapping Case values to period labels
    #     """
    #     # Most date-related things are 1-indexed.
    #     # TODO: these dates will need to be localized (which will include passing in the language).
    #     periodic_ranges = {
    #         'month_of_year': {
    #             'range': xrange(1, 13),
    #             'lookup': lambda x: {'occurred_from__month': x},
    #             'label': lambda x: [
    #                 {
    #                     'text': 'MONTH.{}'.format(calendar.month_name[x].upper()),
    #                     'translate': True
    #                 }
    #             ]
    #         },
    #         'week_of_year': {
    #             'range': xrange(1, 54),  # Up to 53 weeks in a year
    #             'lookup': lambda x: {'occurred_from__week': x},
    #             'label': lambda x: [
    #                 {
    #                     'text': 'AGG.WEEK',
    #                     'translate': True
    #                 },
    #                 {
    #                     'text': str(x),
    #                     'translate': False
    #                 }
    #             ]
    #         },
    #         'day_of_week': {
    #             'range': xrange(1, 8),
    #             'lookup': lambda x: {'occurred_from__week_day': x},
    #             'label': lambda x: [
    #                 {
    #                     'text': self._get_day_label(x),
    #                     'translate': True
    #                 }
    #             ]
    #         },
    #         'day_of_month': {
    #             'range': xrange(1, 32),
    #             'lookup': lambda x: {'occurred_from__day': x},
    #             'label': lambda x: [
    #                 {
    #                     'text': str(x),
    #                     'translate': False
    #                 }
    #             ]
    #         },
    #         'hour_of_day': {
    #             'range': xrange(0, 24),
    #             'lookup': lambda x: {'occurred_from__hour': x},
    #             'label': lambda x: [
    #                 {
    #                     'text': '{}:00'.format(x),
    #                     'translate': False
    #                 }
    #             ]
    #         },
    #     }

    #     # Ranges are built below, partly based on the ranges in 'periodic_ranges' above.
    #     sequential_ranges = {
    #         'year': {
    #             'range': [],
    #             'lookup': lambda x: {'occurred_from__year': x},
    #             'label': lambda x: [
    #                 {
    #                     'text': str(x),
    #                     'translate': False
    #                 }
    #             ]
    #         },
    #         'month': {
    #             'range': [],
    #             'lookup': lambda (yr, mo): {'occurred_from__month': mo, 'occurred_from__year': yr},
    #             'label': lambda (yr, mo): [
    #                 {
    #                     'text': '{}, {}'.format(calendar.month_name[mo], str(yr)),
    #                     'translate': False
    #                 }
    #             ]
    #         },
    #         'week': {
    #             'range': [],
    #             'lookup': lambda (yr, wk): {'occurred_from__week': wk, 'occurred_from__year': yr},
    #             'label': lambda (yr, wk): [
    #                 {
    #                     'text': str(yr),
    #                     'translate': False
    #                 },
    #                 {
    #                     'text': 'AGG.WEEK',
    #                     'translate': True
    #                 },
    #                 {
    #                     'text': str(wk),
    #                     'translate': False
    #                 }
    #             ]
    #         },
    #         'day': {
    #             'range': [],
    #             'lookup': lambda (yr, mo, day): {'occurred_from__month': mo,
    #                                              'occurred_from__year': yr,
    #                                              'occurred_from__day': day},
    #             'label': lambda (yr, mo, day): [
    #                 {
    #                     'text': template_date(datetime.date(yr, mo, day)),
    #                     'translate': False
    #                 }
    #             ]
    #         },
    #     }

    #     if period_type in periodic_ranges.keys():
    #         period = periodic_ranges[period_type]
    #     elif period_type in sequential_ranges.keys():
    #         # Get the desired range, either from the query params or the filtered queryset
    #         if request.query_params.get('occurred_min') is not None:
    #             min_date = parse_date(request.query_params['occurred_min']).date()
    #         else:
    #             min_date = queryset.order_by('occurred_from').first().occurred_from.date()
    #         if request.query_params.get('occurred_max') is not None:
    #             max_date = parse_date(request.query_params['occurred_max']).date()
    #         else:
    #             max_date = queryset.order_by('-occurred_from').first().occurred_from.date()

    #         # Build the relevant range of aggregation periods, based partly on the ones
    #         # already built in 'periodic_ranges' above
    #         sequential_ranges['year']['range'] = xrange(min_date.year, max_date.year + 1)
    #         if period_type != 'year':
    #             # Using the existing lists for 'year' and 'month_of_year', builds a list of
    #             # (year, month) tuples in order for the min_date to max_date range
    #             sequential_ranges['month']['range'] = [
    #                 (year, month) for year in sequential_ranges['year']['range']
    #                 for month in periodic_ranges['month_of_year']['range']
    #                 if min_date <= datetime.date(year, month, calendar.monthrange(year, month)[1])
    #                 and datetime.date(year, month, 1) <= max_date
    #             ]
    #             if period_type == 'day':
    #                 # Loops over the 'month' range from directly above and adds day, to make a
    #                 # list of (year, month, day) tuples in order for the min_date to max_date range
    #                 sequential_ranges['day']['range'] = [
    #                     (year, month, day) for (year, month) in sequential_ranges['month']['range']
    #                     for day in xrange(1, calendar.monthrange(year, month)[1] + 1)
    #                     if min_date <= datetime.date(year, month, day)
    #                     and datetime.date(year, month, day) <= max_date
    #                 ]
    #             elif period_type == 'week':
    #                 # Using the existing lists for 'year' and 'week_of_year', builds a list of
    #                 # (year, week) tuples in order for the min_date to max_date range.
    #                 # Figure out what week the min_date and max_date fall in, then
    #                 # use them as the starting and ending weeks
    #                 def week_start_date(year, week):
    #                     d = datetime.date(year, 1, 1)
    #                     delta_days = d.isoweekday() - 1
    #                     delta_weeks = week
    #                     if year == d.isocalendar()[0]:
    #                         delta_weeks -= 1
    #                     delta = datetime.timedelta(days=-delta_days, weeks=delta_weeks)
    #                     return d + delta

    #                 sequential_ranges['week']['range'] = [
    #                     (year, week) for year in sequential_ranges['year']['range']
    #                     for week in periodic_ranges['week_of_year']['range']
    #                     if week_start_date(
    #                             min_date.year, min_date.isocalendar()[1]
    #                     ) <= week_start_date(year, week)  # include first partial week
    #                     and week_start_date(year, week) <= max_date  # include last partial week
    #                 ]

    #         period = sequential_ranges[period_type]
    #     else:
    #         raise ParseError(detail=('row_/col_period_type must be one of {}; received {}'
    #                                  .format(periodic_ranges.keys() + sequential_ranges.keys(),
    #                                          period_type)))

    #     return self._build_case_from_period(period)

    def _make_ummalqura_period_case(self, period_type, request, queryset):
        periodic_ranges = {
            'month_of_year': {
                'type': 'generated',
                'query': hijri_month_range,
            },
            'week_of_year': {
                'type': 'generated',
                'query': hijri_week_range
            },
            'day_of_week': {
                'type': 'builtin',
                'range': xrange(1, 8),
                'lookup': lambda x: {'occurred_from__week_day': x},
                'label': lambda x: [
                    {
                        'text': self._get_day_label(x),
                        'translate': True
                    }
                ]
            },
            'day_of_month': {
                'type': 'generated',
                'query': hijri_day_range,
            },
            'hour_of_day': {
                'type': 'builtin',
                'range': xrange(0, 24),
                'lookup': lambda x: {'occurred_from__hour': x},
                'label': lambda x: [
                    {
                        'text': '{}:00'.format(x),
                        'translate': False
                    }
                ]
            },
        }

        # Ranges are built below, partly based on the ranges in 'periodic_ranges' above.
        sequential_ranges = {
            'year': {
                'type': 'generated',
                'query': hijri_year_range
            },
            'month': {
                'type': 'generated',
                'query': hijri_month_range
            },
            'week': {
                'type': 'generated',
                'query': hijri_week_range
            },
            'day': {
                'type': 'generated',
                'query': hijri_day_range
            },
        }

        # need to get start/end of every month in the requested range
        # create Q expressions for each month
        # create aggregation for each type of Case query which doesn't translate directly from
        # the gregorian calendar:
        # Periodic: Day of Month, Month of Year, Week of year
        # Sequential: Day, Month, Year

        # Min / max dates are required to limit the # of Q expressions
        if request.query_params.get('occurred_min') is not None:
            min_date = parse_date(request.query_params['occurred_min']).date()
        else:
            min_date = queryset.order_by('occurred_from').first().occurred_from.date()
        if request.query_params.get('occurred_max') is not None:
            max_date = parse_date(request.query_params['occurred_max']).date()
        else:
            max_date = queryset.order_by('-occurred_from').first().occurred_from.date()

        if period_type in periodic_ranges.keys():
            return self._build_ummalqura_periodic_case(
                periodic_ranges, period_type, min_date, max_date
            )
        elif period_type in sequential_ranges.keys():
            return self._build_ummalqura_sequential_case(
                sequential_ranges, period_type, min_date, max_date
            )
        else:
            raise ParseError(detail=('row_/col_period_type must be one of {}; received {}'
                                     .format(periodic_ranges.keys() + sequential_ranges.keys(),
                                             period_type)))

    def _build_ummalqura_periodic_case(self, periodic_ranges, period_type, min_date, max_date):
        period = periodic_ranges[period_type]

        if period['type'] == 'generated':
            query_dates = period['query'](min_date, max_date, True)
            date_sets = query_dates['date_sets']

            whens = []
            labels = []

            for date_set in date_sets:
                range_expressions = []
                for date_range in date_set.ranges:
                    range_expressions.append(
                        (Q(occurred_from__gte=date_range.start) &
                         Q(occurred_from__lt=date_range.end))
                    )
                if len(range_expressions) > 1:
                    in_range = reduce(lambda x, y: x | y, range_expressions)
                elif len(range_expressions) == 1:
                    in_range = range_expressions[0]
                else:
                    continue
                set_when = When(in_range, then=Value(date_set.key))
                whens.append(set_when)
                labels.append({'key': date_set.key,
                               'label': date_set.label})
            return (Case(*whens, output_field=CharField()), labels)

        elif period['type'] == 'builtin':
            return self._build_case_from_period(period)

    def _build_case_from_period(self, period):
        whens = []  # Eventual list of When-clause objects
        for x in period['range']:
            when_args = period['lookup'](x)
            when_args['then'] = Value(str(x))
            whens.append(When(**when_args))

        labels = [{'key': str(x), 'label': period['label'](x)} for x in period['range']]
        return (Case(*whens, output_field=CharField()), labels)

    def _build_ummalqura_sequential_case(self, sequential_ranges, period_type, min_date, max_date):
        period = sequential_ranges[period_type]
        if period and period['type'] == 'generated':
            query_dates = period['query'](min_date, max_date)
            date_sets = query_dates['date_sets']

            whens = []
            labels = []

            for date_set in date_sets:
                # only ever 1 range for each sequential when
                date_range = date_set.ranges[0]
                range_expression = (
                        Q(occurred_from__gte=date_range.start) &
                        Q(occurred_from__lt=date_range.end))
                set_when = When(range_expression, then=Value(date_set.key))
                whens.append(set_when)
                labels.append({'key': date_set.key,
                               'label': date_set.label})
            return (Case(*whens, output_field=CharField()), labels)

        else:
            raise ParseError(
                description='Invalid sequential aggregations type for ummalqura calendar'
            )

    def _make_boundary_case(self, boundary_id):
        """Constructs a Django Case statement for points falling within a particular polygon

        Args:
            boundary_id (uuid): Id of a Boundary whose BoundaryPolygons should be used in the case
        Returns:
            (Case, labels), where Case is a Django Case object outputting the UUID of the polygon
            which contains each record, and labels is a dict mapping boundary pks to their labels.
        """
        boundary = Boundary.objects.get(pk=boundary_id)
        polygons = BoundaryPolygon.objects.filter(boundary=boundary)

        # Sort the polygons by display_field and remove any items that have an empty label
        polygons = sorted([p for p in polygons if p.data[boundary.display_field]],
                          key=lambda p: p.data[boundary.display_field])
        labels = [
            {
                'key': str(poly.pk),
                'label': [
                    {'text': poly.data[boundary.display_field], 'translate': False}
                ]
            }
            for poly in polygons
        ]

        return (Case(*[When(geom__within=poly.geom, then=Value(poly.pk)) for poly in polygons],
                     output_field=UUIDField()), labels)

    def _is_multiple(self, schema, path):
        """Determines whether this related object type has a multiple item configuration

        Args:
            schema (RecordSchema): A RecordSchema to get properties from
            path (list): A list of path fragments to navigate to the desired property
        Returns:
            True if this related object type has a multiple item configuration
        """
        # The related key is always the first item appearing in the path
        try:
            if 'multiple' not in schema.schema['definitions'][path[0]]:
                return False
            return schema.schema['definitions'][path[0]]['multiple']
        except:
            # This shouldn't ever fail, but in case a bug causes the schema to change, treat
            # the related type as non-multiple, since that's the main use-case
            logger.exception('Exception obtaining multiple with path: %s', path)
            return False

    def _make_choices_case(self, schema, path):
        """Constructs a Django Case statement for the choices of a schema property

        Args:
            schema (RecordSchema): A RecordSchema to get properties from
            path (list): A list of path fragments to navigate to the desired property
        Returns:
            (Case, labels), where Case is a Django Case object with the choice of each record,
            and labels is a dict matching choices to their labels (currently the same).
        """

        multiple = self._is_multiple(schema, path)
        choices = self._get_schema_enum_choices(schema, path)
        whens = []
        for choice in choices:
            filter_rule = self._make_djsonb_containment_filter(path, choice, multiple)
            whens.append(When(data__jsonb=filter_rule, then=Value(choice)))
        labels = [
            {'key': choice, 'label': [{'text': choice, 'translate': False}]}
            for choice in choices
        ]
        return (Case(*whens, output_field=CharField()), labels)

    def _get_multiple_choices_annotated_tuple(self, queryset, annotation_id, schema, path):
        """Helper wrapper for annotating a queryset with a case statement

        Args:
          queryset (QuerySet): The input queryset
          annotation_id (String): 'row' or 'col'
          schema (RecordSchema): A RecordSchema to get properties from
          path (list): A list of path fragments to navigate to the desired property

        Returns:
            A 3-tuple of:
              - boolean which specifies whether or not this is a 'multiple' query (always False)
              - dict mapping Case values to labels
              - the newly-annotated queryset
        """

        choices = self._get_schema_enum_choices(schema, path)
        labels = [
            {'key': choice, 'label': [{'text': choice, 'translate': False}]}
            for choice in choices
        ]

        annotations = {}
        for choice in choices:
            filter_rule = self._make_djsonb_containment_filter(path, choice, True)
            annotations['{}_{}'.format(annotation_id, choice)] = Case(
                When(data__jsonb=filter_rule, then=Value(1)),
                output_field=IntegerField(), default=Value(0))

        return (True, labels, queryset.annotate(**annotations))

    # TODO: This snippet also appears in data/serializers.py and should be refactored into the Grout
    # RecordSchema model
    def _get_schema_enum_choices(self, schema, path):
        """Returns the choices in a schema enum field at path

        Args:
            schema (RecordSchema): A RecordSchema to get properties from
            path (list): A list of path fragments to navigate to the desired property
        Returns:
            choices, where choices is a list of strings representing the valid values of the enum.
        """
        # Walk down the schema using the path components
        obj = schema.schema['definitions']  # 'definitions' is the root of all schema paths
        for key in path:
            try:
                obj = obj[key]
            except KeyError as e:
                raise ParseError(
                    detail=u'Could not look up path "{}", "{}" was not found in schema'.format(
                        u':'.join(path), key))

        # Checkbox types have an additional 'items' part at the end of the path
        if 'items' in obj:
            obj = obj['items']

        # Build a JSONB filter that will catch Records that match each choice in the enum.
        choices = obj.get('enum', None)
        if not choices:
            raise ParseError(detail="The property at choices_path is missing required 'enum' field")
        return choices

    def _make_djsonb_containment_filter(self, path, value, multiple):
        """Returns a djsonb containment filter for a path to contain a value

        Args:
            path (list): A list of strings denoting the path
            value (String): The value to match on
            multiple (Boolean): True if this related object type has a multiple item configuration
        Returns:
            A dict representing a valid djsonb containment filter specification that matches if the
            field at path contains value
        """
        # Build the djsonb filter specification from the inside out, and skip schema-only keys, i.e.
        # 'properties' and 'items'.
        filter_path = [component for component in reversed(path)
                       if component not in ['properties', 'items']]

        # Check if a row contains either the value as a string, or the value in an array.
        # The string value handles dropdown types, while the array handles checkbox types.
        # Since an admin may switch between dropdowns and checkboxes at any time, performing
        # both checks guarantees the filter will be correctly applied for data in both formats.
        rule_type = 'containment_multiple' if multiple else 'containment'
        filter_rule = dict(_rule_type=rule_type, contains=[value, [value]])
        for component in filter_path:
            # Nest filter inside itself so we eventually get something like:
            # {"incidentDetails": {"severity": {"_rule_type": "containment"}}}
            tmp = dict()
            tmp[component] = filter_rule
            filter_rule = tmp
        return filter_rule


class MakeZipOfData(APIView):
    def post(self, request):
        dateTimeObj = datetime.datetime.now()
        current_date_time = str(dateTimeObj).replace(" ", "").replace("-", "").replace(":", "").replace(".", "_")
        request_data = request.data
        json_data = request_data["jsondata"]
        csv_data = request_data["csvdata"]
        data_folder = os.path.join(os.getcwd(), "usersdata")
        if not os.path.exists(data_folder):
            os.mkdir(data_folder)

        os.makedirs("/var/www/media", exist_ok=True)
        json_file_name = os.path.join("/var/www/media", "collision_diagram_" + str(current_date_time) + ".geojson")
        csv_file_name = os.path.join("/var/www/media", "collision_diagram_" + str(current_date_time) + ".csv")
        zip_file_name = "collision_diagram_" + str(current_date_time) + ".zip"
        ######### Write json data in geojson file #####
        with open(json_file_name, 'w') as fp:
            json.dump(json_data, fp)
        if ":" in request.META['HTTP_HOST']:
            host_ip = request.META['HTTP_HOST'].split(":")[0]
        else:
            host_ip = request.META["SERVER_NAME"]
        datalist = []
        datacount = 0
        response_data = csv_data
        if response_data:
            datakeys = response_data[0]
            keys_list = []
            removekeys = ["uuid", "geom", "created", "modified", "occurred_to"]
            for i in datakeys:
                if i not in removekeys:
                    keys_list.append(i)
            for dataitem in response_data:
                datadict = {}
                datacount += 1
                datadict["Crash number"] = datacount
                for keyitem in keys_list:
                    if keyitem == "data":
                        for nesteditem in dataitem[keyitem]:
                            for itemkey in dataitem[keyitem][nesteditem]:
                                if itemkey in ["_localId", "Description"]:
                                    pass
                                else:
                                    if nesteditem == "driverPhoto":
                                        pass
                                    else:
                                        if nesteditem == "driverVehicle":
                                            if '_localId' in dataitem[keyitem][nesteditem][0]:
                                                dataitem[keyitem][nesteditem][0].pop("_localId")
                                            demodict = {}
                                            for i in dataitem[keyitem][nesteditem][0].keys():
                                                demodict[i] = []
                                            obj_count = 0
                                            direction_count = 0
                                            for i in dataitem[keyitem][nesteditem]:
                                                obj_count += 1
                                                direction_count += 1
                                                if ("Vehicle type" or "Direction") in i:
                                                    if "Vehicle type" in i:
                                                        try:
                                                            datadict["Object" + " " + str(obj_count)] = i[
                                                                "Vehicle type"]
                                                        except:
                                                            datadict["Object" + " " + str(obj_count)] = None
                                                    else:
                                                        datadict["Object" + " " + str(obj_count)] = None
                                                    if "Direction" in i:
                                                        try:
                                                            datadict["Direction" + " " + str(direction_count)] = i[
                                                                "Direction"]
                                                        except:
                                                            datadict["Direction" + " " + str(direction_count)] = None
                                                    else:
                                                        datadict["Direction" + " " + str(direction_count)] = None
                                                if "Surface" in i:
                                                    datadict["Road Condition"] = i["Surface"]
                                                for nestedkey in demodict:
                                                    if i[nestedkey]:
                                                        demodict[nestedkey].append(i[nestedkey])
                                            for key, value in demodict.items():
                                                datadict[key] = value
                                        else:
                                            if type(dataitem[keyitem][nesteditem]) == list:
                                                for listitem in dataitem[keyitem][nesteditem]:
                                                    if "_localId" in listitem:
                                                        listitem.pop("_localId")
                                                    else:
                                                        for li in listitem:
                                                            datadict[str(li)] = listitem[li]
                                            else:
                                                valuedata = dataitem[keyitem][nesteditem][str(itemkey)]
                                                if 'data:image' in valuedata:
                                                    pass
                                                else:
                                                    if str(itemkey) == "Movement Code":
                                                        datadict["DCA code"] = valuedata
                                                    else:
                                                        datadict[str(itemkey)] = valuedata
                    else:
                        if keyitem == "occurred_from":
                            occured_date = dataitem["occurred_from"].split("T")
                            year, month, day = (int(i) for i in occured_date[0].split('-'))
                            dayNumber = calendar.weekday(year, month, day)
                            days = ["Monday", "Tuesday", "Wednesday", "Thursday",
                                    "Friday", "Saturday", "Sunday"]
                            day_of_week = days[dayNumber]
                            if len(str(day)) == 1:
                                date = str(0) + str(day)
                            else:
                                date = str(day)
                            if len(str(month)) == 1:
                                month = str(0) + str(month)
                            else:
                                month = str(month)
                            day_month = date + "-" + month
                            time_of_day = occured_date[1].split(":")[0] + ":" + occured_date[1].split(":")[1]
                            datadict["Day of week"] = day_of_week
                            datadict["Date:day-month"] = day_month
                            datadict["Date:year"] = year
                            datadict["Time of day"] = time_of_day
                        else:
                            valuedata = dataitem[str(keyitem)]
                            datadict[str(keyitem)] = valuedata

                if 'Severity' in datadict:
                    severity = datadict['Severity']
                    if type(severity) == list:
                        datadict['Severity'] = "|".join(severity)

                datalist.append(datadict)
            keys_list = ["Crash number", "Day of week", "Date:day-month", "Date:year",
                         "Time of day", "Severity", "light", "Road Condition", "DCA code", "Object", "Direction"]
            for singlejson in datalist:
                for i in list(singlejson):
                    if i == "Direction":
                        singlejson.pop(i)
                    elif i.startswith("Object"):
                        pass
                    elif i.startswith("Direction"):
                        pass
                    elif i in keys_list:
                        pass
                    else:
                        singlejson.pop(i)
            maxkeys = []
            for i in datalist:
                maxkeys.append(len(i))
            indexno = maxkeys.index(max(maxkeys))
            for i in datalist[indexno].keys():
                if i not in datalist[0]:
                    datalist[0][i] = None

            all_keys_as_columns = []
            list_final = []
            if datalist:
                all_keys_as_columns_fromdict = datalist[0].keys()

                objects_list = []
                directions_list = []
                for col in all_keys_as_columns_fromdict:
                    if col.startswith("Objec"):
                        objects_list.append(col)
                    elif col.startswith("Directio"):
                        directions_list.append(col)
                    else:
                        all_keys_as_columns.append(col)

                objects_list.sort()
                directions_list.sort()

                all_keys_as_columns.extend(objects_list)
                all_keys_as_columns.extend(directions_list)

                for i in datalist:
                    mini_final_list = []
                    for column_name in all_keys_as_columns:
                        if column_name in i:
                            mini_final_list.append(i[column_name])
                        else:
                            mini_final_list.append(None)
                    list_final.append(mini_final_list)
        else:
            list_final = []
            all_keys_as_columns = []
        pddata = pd.DataFrame(list_final, columns=all_keys_as_columns)
        pddata.to_csv(csv_file_name, index=0)
        ######### End of Write csv data file #####
        ########### Make Zip of both files ##########
        zip_store_path = os.path.join("/var/www/media/", zip_file_name)
        # zip_store_path = os.path.join(data_folder, zip_file_name)
        zip_obj = ZipFile(zip_store_path, 'w')
        zip_obj.write(json_file_name, os.path.basename(json_file_name))
        zip_obj.write(csv_file_name, os.path.basename(csv_file_name))
        zip_obj.close()
        whole_path = os.path.join(str(settings.HOST_URL), zip_file_name)
        os.unlink(json_file_name)
        os.unlink(csv_file_name)
        return Response({'filename': whole_path})


class SaveIrapInformation(APIView):

    @transaction.atomic
    def post(self, request):
        base_url = "http://toolkit.irap.org/api/?content=treatments"
        token = request.META.get('HTTP_AUTHORIZATION')
        try:
            response = requests.get(base_url,
                                    headers={"Authorization": token}
                                    )
            rectype_id = response.json()
            for data in rectype_id:
                irap_detail = IrapDetail(irap_treatment_id=data['id'],
                                         irap_treatment_name=data['name'],
                                         path=data['path'])
                irap_detail.save()
        except Exception as e:
            raise e
        return Response({'data': 'Success'})


class DedupeDistanceConfigViewSet(drf_mixins.ListModelMixin, drf_mixins.RetrieveModelMixin,
                                  drf_mixins.UpdateModelMixin, viewsets.GenericViewSet):
    """ViewSet for Dedupe Distance configuration
    The DedupeDistanceConfig object is designed to be a singleton, so POST and DELETE are disabled.
    """
    serializer_class = DuplicateDistanceConfigSerializer
    pagination_class = None

    def get_queryset(self):
        """Ensure that we always return a single config object"""
        # This is a bit roundabout, but we have to return a queryset rather than an object.
        config = DuplicateDistanceConfig.objects.all().order_by('pk').first()
        if not config:
            DuplicateDistanceConfig.objects.create()
            config = DuplicateDistanceConfig.objects.all().order_by('pk').first()
        return DuplicateDistanceConfig.objects.filter(pk__in=[config.pk])


class BulkUploadDetailViewSet(viewsets.ModelViewSet):
    serializer_class = BulkUploadDetailSerializer
    pagination_class = None

    def get_queryset(self):
        return BulkUploadDetail.objects.all().order_by('-csv_uploaded_date')


class RecordMapDetais(viewsets.ModelViewSet):
    serializer_class = DriverRecordMapSerializer
    permissions_classes = [IsAuthenticated, ]
    filter_class = filters.DriverRecordFilter
    # filter_backends = (SearchFilter,)
    # search_fields = ['weather', 'light', 'city', 'city_district', 'county', 'road', 'state', 'location_text',
    #                  'data', ]
    # queryset = DriverRecord.objects.all()
    pagination_class = OptionalLimitOffsetPagination

    def get_queryset(self):
        return DriverRecord.objects.all()


class WeatherDataListViewset(viewsets.ModelViewSet):
    serializer_class = WeatherDataListSerializer
    permission_classes = [IsAuthenticated, ]
    filter_class = filters.WeatherDataListFilter
    pagination_class = None

    def get_queryset(self):
        return WeatherDataList.objects.all()

    def perform_create(self, serializer):
        id_list = self.request.data.get('ids')
        for obj in WeatherDataList.objects.all():
            if obj.id in id_list:
                obj.active = True
                obj.save()
            else:
                obj.active = False
                obj.save()


    def perform_update(self, serializer):
        serializer.save()

    def perform_destroy(self, instance):
        instance.delete()

class KeyDetailsViewSet(viewsets.ModelViewSet):
    serializer_class = KeyDetailSerializer
    permission_classes = []
    pagination_class = None

    def get_queryset(self):
        return KeyDetail.objects.all()
