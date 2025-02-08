from rest_framework import viewsets
from rest_framework import mixins as drf_mixins
from rest_framework.response import Response
from django_redis import get_redis_connection
from django.utils import timezone

from data.models import DriverRecord, RecordType
from black_spots.models import (BlackSpot, BlackSpotSet, BlackSpotConfig)
from black_spots.serializers import (BlackSpotSerializer, BlackSpotSetSerializer,
                                     BlackSpotConfigSerializer, EnforcerAssignmentInputSerializer,
                                     EnforcerAssignmentSerializer)
from black_spots.filters import (BlackSpotFilter, BlackSpotSetFilter, EnforcerAssignmentFilter)
from data.views import build_toddow
from data.tasks.load_black_spots import load_black_spots

# from driver_auth.permissions import IsAdminOrReadOnly
from DRIVER import mixins
import datetime
import random
import uuid
from dateutil import rrule
import base64
import time
from rest_framework.decorators import api_view
from utility.response_utils import ok_response, error_response
import os
from scripts.bulk_upload.load_black_spots import main as black_spot_main
# from scripts.bulk_upload.load_incidents_v3 import main as incident_main
from scripts.bulk_upload.load_interventions import main as intervention_main
from scripts.core.incident_validation import incidentvalidation
from scripts.core.democsv import democsv_sample, file_for_intervention
from scripts.bulk_upload.add_intervention_data import add_interventions
from django.conf import settings


class BlackSpotViewSet(viewsets.ModelViewSet, mixins.GenerateViewsetQuery):
    """ViewSet for black spots"""
    queryset = BlackSpot.objects.all()
    serializer_class = BlackSpotSerializer
    filter_class = BlackSpotFilter
    # permission_classes = (IsAdminOrReadOnly, )
    pagination_class = None

    def list(self, request, *args, **kwargs):
        # if tilekey if specified, use to get query
        response = Response(None)
        if ('tilekey' in request.query_params):
            tile_token = request.query_params['tilekey']
            redis_conn = get_redis_connection('default')
            sql = redis_conn.get(tile_token)
            if sql:
                tilekey_queryset = BlackSpot.objects.raw(sql)
                tilekey_serializer = BlackSpotSerializer(tilekey_queryset, many=True)
                tilekey_serializer.data.insert(0, {'count': len(tilekey_serializer.data)})
                response = Response(tilekey_serializer.data)
        else:
            response = super(BlackSpotViewSet, self).list(self, request, *args, **kwargs)
        return response


class BlackSpotSetViewSet(viewsets.ModelViewSet):
    """ViewSet for black spot sets"""
    queryset = BlackSpotSet.objects.all()
    serializer_class = BlackSpotSetSerializer
    filter_class = BlackSpotSetFilter
    # permission_classes = (IsAdminOrReadOnly, )
    pagination_class = None

    def list(self, request, *args, **kwargs):
        response = super(BlackSpotSetViewSet, self).list(self, request, *args, **kwargs)
        # If a polygon is passed as an argument, return a tilekey instead of a BlackSpotSet
        # Store the required SQL to filter Blackspots on that polygon
        if 'polygon' in request.query_params and len(response.data['results']) > 0:
            request.uuid = response.data['results'][0]['uuid']
            query_sql = BlackSpotViewSet().generate_query_sql(request)
            tile_token = uuid.uuid4()
            redis_conn = get_redis_connection('default')
            redis_conn.set(tile_token, query_sql.encode('utf-8'))
            # return tile_token instead of the BlackspotSet uuid
            response = Response({'count': 1, 'results': [{'tilekey': tile_token}]})
        return response


class BlackSpotConfigViewSet(drf_mixins.ListModelMixin, drf_mixins.RetrieveModelMixin,
                             drf_mixins.UpdateModelMixin, viewsets.GenericViewSet):
    """ViewSet for BlackSpot configuration
    The BlackSpotConfig object is designed to be a singleton, so POST and DELETE are disabled.
    """
    serializer_class = BlackSpotConfigSerializer
    pagination_class = None

    def get_queryset(self):
        """Ensure that we always return a single config object"""
        # This is a bit roundabout, but we have to return a queryset rather than an object.
        config = BlackSpotConfig.objects.all().order_by('pk').first()
        if not config:
            BlackSpotConfig.objects.create()
            config = BlackSpotConfig.objects.all().order_by('pk').first()
        return BlackSpotConfig.objects.filter(pk__in=[config.pk])


class EnforcerAssignmentViewSet(drf_mixins.ListModelMixin, viewsets.GenericViewSet):
    """ViewSet for enforcer assignments"""
    # Enforcer assignments do not currently have an associated model in the database, and are
    # instead based direcly on black spots, so this is a list-only ViewSet. This was created
    # as its own ViewSet to make it easier if we ever decide to create a db model for
    # enforcer assignments.

    queryset = BlackSpot.objects.all()
    serializer_class = EnforcerAssignmentSerializer
    filter_class = EnforcerAssignmentFilter
    pagination_class = None

    # permission_classes = (IsAdminOrReadOnly, )

    def choose_assignments(self, assignments, num_personnel, shift_start, shift_end):
        """
        Select the assignments according to the supplied parameters
        :param assignments: filtered queryset of assignments (black spots)
        :param num_personnel: number of assignments to choose
        :param shift_start: start dt of the shift
        :param shift_end: end dt of the shift
        """

        # The multiplier used for determining the set of assignments to choose from. The size of
        # the list of possible assignments is determined by multiplying this number by the number of
        # personnel. A higher number will result in a greater variance of assignments among shifts.
        fuzz_factor = 4

        # Create a set of assignments with the highest forecasted severity score to sample from.
        assignments = assignments.order_by('-severity_score')[:num_personnel * fuzz_factor]

        # Specify a random seed based on the shift, so assignments are deterministic during the same
        # shift, yet vary from shift to shift.
        random.seed(hash('{}_{}'.format(shift_start, shift_end)))

        # Return the sampled list of assignments
        if num_personnel < len(assignments):
            return random.sample(list(assignments), num_personnel)
        return assignments

    def scale_by_toddow(self, assignments, shift_start, shift_end, record_type, geom):
        """
        Scale the expected load forecast (severity score) by the time of day and day of week
        :param assignments: filtered queryset of assignments (black spots)
        :param shift_start: start dt of the shift
        :param shift_end: end dt of the shift
        :record_type: the record type uuid
        :geom: the geometry object used for filtering records in toddow creation
        """

        # Generate the ToDDoW aggregation using the past year of data
        num_days_events = 365
        max_dt = timezone.now()
        min_dt = max_dt - datetime.timedelta(days=num_days_events)
        records = DriverRecord.objects.filter(
            occurred_from__gte=min_dt,
            occurred_to__lte=max_dt,
            schema__record_type_id=record_type
        )
        if geom:
            records = records.filter(geom__intersects=geom)
        toddow = build_toddow(records)

        # Construct an `rrule` for iterating over hours between shift start and shift end.
        # Each of these items will be matched up to the items returned in the toddow aggregation to
        # determine which toddow buckets the shift includes. So we need to ensure that the maximum
        # hours in the range is 7x24 to make sure nothing is double counted.
        max_shift_end = shift_start + datetime.timedelta(hours=7 * 24)
        shift_end = min(shift_end, max_shift_end)
        # If the shift_end falls exactly on on hour mark, don't include that bucket
        if shift_end.second == 0 and shift_end.minute == 0:
            shift_end = shift_end - datetime.timedelta(microseconds=1)
        hour_generator = rrule.rrule(rrule.HOURLY, dtstart=shift_start, until=shift_end)

        # Iterate over the ToDDoW items and determine which ones are relevant to this shift
        total_count, in_shift_count = 0, 0
        for item in toddow:
            count = item['count']
            total_count += count
            for hourly_dt in hour_generator:
                if hourly_dt.hour == item['tod'] and hourly_dt.isoweekday() == item['dow']:
                    in_shift_count += count
                    break

        # Use ratio of in_shift_count to total_count as the scaling factor
        if total_count > 0 and in_shift_count > 0:
            scaling_factor = in_shift_count / float(total_count)
        else:
            # If there aren't enough counts to properly determine a scaling factor,
            # base it on the linear number of toddow buckets.
            scaling_factor = len(list(hour_generator)) / (7 * 24.0)

        # Need to divide by 52, since the ToDDoW proportion only represents a weekly aggregation,
        # yet the severity score is a yearly figure.
        scaling_factor /= 52

        # Scale the severity score by the scaling factor
        for assignment in assignments:
            assignment.severity_score *= scaling_factor

        return assignments

    def list(self, request, *args, **kwargs):
        """
        List endpoint for enforcer assignments.
        Required URL parameters:
            - record_type - uuid of the record type
            - num_enforcers - number of enforcer assignments to generate
            - shift_start - start dt of the shift
            - shift_end - end dt of the shift
        Optional URL parameters:
            - polygon - WKT for the polygon to generate enforcer assignments for
            - polygon_id - uuid of the polygon to generate enforcer assignments for

        :param request:  The request object
        """

        input_serializer = EnforcerAssignmentInputSerializer(request)
        num_personnel = input_serializer.num_personnel
        shift_start = input_serializer.shift_start
        shift_end = input_serializer.shift_end
        record_type = input_serializer.record_type
        geom = input_serializer.geom

        # Filter the assignments by supplied parameters, sample them, and scale by ToDDoW
        assignments = self.filter_queryset(self.get_queryset())
        assignments = self.choose_assignments(assignments, num_personnel, shift_start, shift_end)
        assignments = self.scale_by_toddow(assignments, shift_start, shift_end, record_type, geom)

        output_serializer = self.get_serializer(assignments, many=True)
        return Response(output_serializer.data)


# @api_view(['POST'])
# def bulkupload(request):
#     try:
#         # file_value = request.data["file"]
#         file_value = request.FILES["file"]
#     except:
#         return error_response(message="File not found")
#
#     try:
#         upload_for = request.data["upload_for"]
#     except:
#         return error_response(message="json key error")
#
#     # import ipdb;ipdb.set_trace()
#     if upload_for not in ["black_spots", "interventions", "Incident"]:
#         return error_response("Wrong value for file upload")
#
#     json_folder_path = os.path.join(os.getcwd(), upload_for+"_json")
#     if not os.path.exists(json_folder_path):
#         os.makedirs(json_folder_path)
#
#     fs = FileSystemStorage()
#     jsonpath = os.path.join(json_folder_path, file_value.name)
#     fs.save(jsonpath, file_value)
#
#     api_url = request.META['HTTP_HOST']
#     headers = {}
#     headers_for_post = {'content-type': 'application/json'}
#     now = datetime.now().isoformat() + 'Z'
#     headers['Authorization'] = request.META["HTTP_AUTHORIZATION"]
#     headers_for_post['Authorization'] = request.META["HTTP_AUTHORIZATION"]
#
#     args_dict = {"apiurl":api_url, "jsonpath":jsonpath,
#                  "header":headers, "post_header":headers_for_post, "current_time":now}
#
#     if upload_for == "black_spots":
#         black_spot_main(**args_dict)
#         return ok_response()
#     elif upload_for == "intervention":
#         intervention_main(**args_dict)
#     elif upload_for == "Incident":
#         # incident_main(**args_dict)
#         # black_spot_main(**args_dict)
#         # return ok_response()
#         pass


@api_view(['POST'])
def json_encode(request):
    from datetime import datetime
    request_data = request.data
    url = str(settings.HOST_URL)
    protocol = url.split('/')[0]
    try:
        upload_for = request.data["upload_for"]
    except:
        return error_response(message="json key error")

    if upload_for not in ["black_spots", "intervention", "incident"]:
        return error_response("Wrong value for file upload")

    current_time = str(time.time()).split(".")[0]
    try:
        if upload_for == "black_spots":

            base64_message = request_data['jsondata']

            # base64_message = base64_message.split("data:application/json;base64")[1]

            base64_message = base64_message.split(";")[1].split("base64,")[1]

            json_folder_path = os.path.join(os.getcwd(), "scripts", upload_for + "_json")
            if not os.path.exists(json_folder_path):
                os.makedirs(json_folder_path)

            base64_img_bytes = base64_message.encode('utf-8')
            jsonpath = os.path.join(json_folder_path, current_time + '.json')
            with open(jsonpath, 'wb') as file_to_save:
                decoded_image_data = base64.decodebytes(base64_img_bytes)
                file_to_save.write(decoded_image_data)

            api_url = request.META['HTTP_HOST']
            headers = {}
            headers_for_post = {'content-type': 'application/json'}
            now = datetime.now().isoformat() + 'Z'
            headers['Authorization'] = request.META["HTTP_AUTHORIZATION"]
            headers_for_post['Authorization'] = request.META["HTTP_AUTHORIZATION"]

            args_dict = {"apiurl": api_url, "jsonpath": jsonpath,
                         "header": headers, "post_header": headers_for_post, "current_time": now}

            # black_spot_main(**args_dict)
            # load_black_spots()
            return ok_response()
        elif upload_for == "intervention":
            try:
                base64_message = request_data['jsondata']
                record_type = request_data['record_type']
            except:
                return error_response(message="json key error")

            base64_message = base64_message.split("data:text/csv;base64")[1]
            csv_folder_path = os.path.join(os.getcwd(), "scripts", upload_for + "_csvs")
            if not os.path.exists(csv_folder_path):
                os.makedirs(csv_folder_path)

            base64_img_bytes = base64_message.encode('utf-8')
            csvpath = os.path.join(csv_folder_path, current_time + '.csv')
            with open(csvpath, 'wb') as file_to_save:
                decoded_image_data = base64.decodebytes(base64_img_bytes)
                file_to_save.write(decoded_image_data)

            api_url = request.META['HTTP_HOST']
            headers = {}
            headers_for_post = {'content-type': 'application/json'}
            now = datetime.now().isoformat() + 'Z'
            headers['Authorization'] = request.META["HTTP_AUTHORIZATION"]
            headers_for_post['Authorization'] = request.META["HTTP_AUTHORIZATION"]
            filesavepath = str(settings.STATIC_ROOT) + "media/intervention_errorlog_data"
            if not os.path.exists(filesavepath):
                os.makedirs(filesavepath)

            logfilepath = os.path.join(filesavepath, current_time + ".xlsx")

            if ":" in request.META['HTTP_HOST']:
                host_ip = request.META['HTTP_HOST'].split(":")[0]
            else:
                host_ip = request.META["SERVER_NAME"]
            whole_path = str(settings.HOST_URL) + os.path.join("/download", "intervention_errorlog_data", current_time + ".xlsx")

            args_dict = {"apiurl": api_url, "csvpath": csvpath, "record_type": record_type,
                         "logfilepath": logfilepath, "header": headers, "whole_path": whole_path,
                         "post_header": headers_for_post, "current_time": now, "protocol": protocol}

            # intervention_main(**args_dict)
            return add_interventions(**args_dict)

        elif upload_for == "incident":
            try:
                base64_message = request_data['jsondata']
                record_type = request_data['record_type']
            except:
                return error_response(message="json key error")


            base64_message = base64_message.split(";")[1].split("base64,")[1]
            usertoken = request.META["HTTP_AUTHORIZATION"]

            json_folder_path = os.path.join(os.getcwd(), "scripts", upload_for + "_csvs")
            if not os.path.exists(json_folder_path):
                os.makedirs(json_folder_path)

            base64_img_bytes = base64_message.encode('utf-8')
            csvpath = os.path.join(json_folder_path, current_time + '.csv')
            with open(csvpath, 'wb') as file_to_save:
                decoded_image_data = base64.decodebytes(base64_img_bytes)
                file_to_save.write(decoded_image_data)

            filesavepath = str(settings.STATIC_ROOT) + "media/incident_errorlog_data"
            if not os.path.exists(filesavepath):
                os.makedirs(filesavepath)

            logfilepath = os.path.join(filesavepath, current_time + ".xlsx")

            if ":" in request.META['HTTP_HOST']:
                host_ip = request.META['HTTP_HOST'].split(":")[0]
            else:
                host_ip = request.META["SERVER_NAME"]
            whole_path = str(settings.HOST_URL) + os.path.join("/download", "incident_errorlog_data", current_time + ".xlsx")
            api_url = request.META['HTTP_HOST']

            args_dict = {"uploadedcsv": csvpath, "logfilepath": logfilepath,
                         "returnpath": whole_path, "apiurl": api_url,
                         "record_type": record_type, "usertoken": usertoken, "protocol": protocol}

            return incidentvalidation(**args_dict)

            # api_url = request.META['HTTP_HOST']
            # headers = {}
            # headers_for_post = {'content-type': 'application/json'}
            # now = datetime.now().isoformat() + 'Z'
            # headers['Authorization'] = request.META["HTTP_AUTHORIZATION"]
            # headers_for_post['Authorization'] = request.META["HTTP_AUTHORIZATION"]
            #
            # args_dict = {"apiurl": api_url, "header": headers, "csvpath":csvpath,
            #              "post_header": headers_for_post, "current_time": now}
            # incident_main(**args_dict)
            # return ok_response()
    except:
        # base64_message = request_data['jsondata']
        # base64_message = base64_message.split("data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64")[1]
        # base64_img_bytes = base64_message.encode('utf-8')
        # with open('JSON_NAME.xlsx', 'wb') as file_to_save:
        #     decoded_image_data = base64.decodebytes(base64_img_bytes)
        #     file_to_save.write(decoded_image_data)
        # return ok_response()
        return error_response(message="something went wrong")


@api_view(['POST'])
def csvsample(request):
    request_data = request.data
    api_url = request.META['HTTP_HOST']
    url = str(settings.HOST_URL)
    protocol = url.split('/')[0]
    usertoken = request.META["HTTP_AUTHORIZATION"]

    try:
        record_type = request_data['record_type']
    except:
        return error_response(message="json key error")

    if ":" in api_url:
        host_ip = api_url.split(":")[0]
    else:
        host_ip = request.META["SERVER_NAME"]

    obj = RecordType.objects.get(uuid=record_type)
    filesavepath = str(settings.STATIC_ROOT) + "media"
    if obj.label == "Intervention":
        csv_file = "sample_csv_intervention.csv"
        whole_path = os.path.join(filesavepath, csv_file)
        args_dict = {"apiurl": api_url, "record_type": record_type, "usertoken": usertoken, "csvfile": whole_path,
                     "protocol": protocol}
        return file_for_intervention(**args_dict)
    elif obj.label == "Incident":
        csv_file = "sample_csv.csv"

        whole_path = os.path.join(filesavepath, csv_file)

        args_dict = {"apiurl": api_url, "record_type": record_type, "usertoken": usertoken, "csvfile": whole_path,
                     "protocol": protocol}
        return democsv_sample(**args_dict)
