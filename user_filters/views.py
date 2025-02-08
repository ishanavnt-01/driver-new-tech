from rest_framework import viewsets
from rest_framework.views import APIView
from user_filters.serializers import SavedFilterSerializer
from data.serializers import RecordSchemaSerializer
from rest_framework.permissions import IsAuthenticated
# from driver_auth.permissions import IsOwnerOrAdmin
from grout.models import Boundary, BoundaryPolygon, Record, RecordType, RecordSchema
from rest_framework.response import Response
import json
import requests
from DRIVER import settings
# from django.conf import settings
from django.contrib.auth.models import User, Group
from data.models import IrapDetail


class SavedFilterViewSet(viewsets.ModelViewSet):
    serializer_class = SavedFilterSerializer
    permission_classes = (IsAuthenticated,)
    pagination_class = None

    def get_queryset(self):
        return self.request.user.savedfilter_set.all()

    def perform_create(self, serializer):
        serializer.save(owner=self.request.user)


class BindVehicalFilterViewSet(APIView):

    def get(self, request, uuids):
        queryset = RecordSchema.objects.filter(record_type=uuids)
        serializer = RecordSchemaSerializer(queryset, many=True)
        array = serializer.data[0]["schema"]["definitions"]["driverVehicle"]["properties"]["Vehicle type"]
        # return Response({"data": serializer.data.data[0], "message": "Success", "status": True})
        return Response({"data": array})


class BindIncidentDetailFilterViewSet(APIView):

    def post(self, request, uuids, headers=None):
        nested_filter = []
        irap_tratments_list = []
        i = 0

        base_url = str(settings.HOST_URL) + "/data-api/latestrecordschema/"
        token = request.META.get('HTTP_AUTHORIZATION')
        response = requests.post(base_url,
                                 data={"record_type_id": uuids},
                                 headers={"Authorization": token}
                                 )
        rectype_id = response.json()["result"][0]['uuid']

        queryset = RecordSchema.objects.filter(uuid=rectype_id)
        serializer = RecordSchemaSerializer(queryset, many=True)
        group_id = request.data['group_id']
        group_name = str(Group.objects.get(pk=group_id))

        if group_name == settings.Read_Only_group and settings.export_csv_keyname in serializer.data[0]["schema"][
            "definitions"]:

            array = serializer.data[0]["schema"]["definitions"][settings.export_csv_keyname]["properties"]
            for (key, value) in array.items():

                if "isSearchable" in value:
                    if value["isSearchable"] == True:
                        nested_filter.append({key: value})

                else:
                    pass


        else:

            for key_parent, value_parent in (serializer.data[0]["schema"]["definitions"]).items():
                array = serializer.data[0]["schema"]["definitions"][key_parent]["properties"]

                if key_parent == "driverInterventionDetails":
                    nested_filter.append({"Type": array["Type"]})
                    element_list = [element for element in array['Type']['enum'] if element.isdigit()]
                    for element in element_list:
                        array['Type']['enum'].remove(element)
                        irap_instance = IrapDetail.objects.get(irap_treatment_id=element)
                        irap_tratments_list.append(irap_instance.irap_treatment_name)
                    for irap_tratments in irap_tratments_list:
                        array['Type']['enum'].insert(i, irap_tratments)
                        i += 1
                    # array['Type']['enum'].insert(0, irap_tratments_list)
                else:
                    for (key, value) in array.items():

                        if "isSearchable" in value:
                            if value["isSearchable"] == True:
                                nested_filter.append({key: value})

                        else:
                            pass
        return Response(nested_filter)


class BindInterventionSelection(APIView):
    # how to get dynamic JSON object is remaining

    def get(self, request, uuids):
        nested_filter = []
        queryset = RecordSchema.objects.filter(record_type=uuids)
        serializer = RecordSchemaSerializer(queryset, many=True)
        array = serializer.data[0]["schema"]["definitions"]["driverInterventionDetails"]["properties"]
        nested_filter.append({"Type": array["Type"]})

        return Response(nested_filter)


class InterventionDetailType(APIView):

    def post(self, request):
        treatment_name = request.data.get('intervention_type')
        try:
            if isinstance(treatment_name, int):
                intervention_object = IrapDetail.objects.get(irap_treatment_id=treatment_name)
                return Response({"intervention_detail": intervention_object.irap_treatment_name})
            else:
                intervention_object = IrapDetail.objects.get(irap_treatment_name=treatment_name)
                return Response({"intervention_detail": intervention_object.irap_treatment_id})
        except IrapDetail.DoesNotExist:
            return Response({"intervention_detail": treatment_name})
