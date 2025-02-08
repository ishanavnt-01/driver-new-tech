# from celery.task import periodic_task
import requests
from data.models import IrapDetail
from rest_framework.response import Response
from celery.schedules import crontab
from datetime import timedelta


# @periodic_task(run_every=(timedelta(hours=168)), name="update_irap_data")
# def update_irap_data():
#     base_url = "http://toolkit.irap.org/api/?content=treatments"
#     # token = request.META.get('HTTP_AUTHORIZATION')
#     try:
#         response = requests.get(base_url)
#         rectype_id = response.json()
#         for data in rectype_id:
#             if IrapDetail.objects.filter(irap_treatment_id=data['id']).exists():
#                 irap_object = IrapDetail.objects.get(irap_treatment_id=data['id'])
#                 if irap_object.irap_treatment_name != data['name'] or irap_object.path != data['path']:
#                     irap_object.irap_treatment_name = data['name']
#                     irap_object.path = data['path']
#                     irap_object.save()
#             else:
#                 irap_detail = IrapDetail(irap_treatment_id=data['id'],
#                                          irap_treatment_name=data['name'],
#                                          path=data['path'])
#                 irap_detail.save()
#     except Exception as e:
#         raise e
#     return Response({'data': 'Success'})
