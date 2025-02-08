from data import views as data_views
from rest_framework.urlpatterns import format_suffix_patterns
from django.urls import re_path

urlpatterns = [
    re_path('recordtype/', data_views.RecordTypeViewSet.as_view()),
    re_path('latestrecordschema/', data_views.GetLatestRecordSchema.as_view()),
    re_path('latestrecordcost/', data_views.GetLatestRecordcost.as_view()),
    re_path('costs/', data_views.RecordCost.as_view()),
    re_path('getjsonb/(?P<uuids>[^/]+)/$', data_views.CreateJsonViewSet.as_view()),
    re_path('getbargraph', data_views.WeeklyBarGraph.as_view()),
    re_path('bindcrashtype/(?P<uuids>[^/]+)/$', data_views.BindCrashTypeViewSet.as_view()),
    re_path('addcrashorientation/', data_views.CreateCrashDiagramViewset.as_view()),
    re_path('getcrashorientationbymovementcode/', data_views.GetCrashDiagramOrientationViewset.as_view()),
    re_path('updatecrashdata/(?P<uuids>[^/]+)/$', data_views.UpdateCrashDiagramViewset.as_view()),
    re_path('deletemovementcode/', data_views.DeleteMovementCodeAsPerCrashTypeViewset.as_view()),
    re_path('savedata/', data_views.MakeZipOfData.as_view()),
    re_path(r'save-irap-details/', data_views.SaveIrapInformation.as_view()),
]

urlpatterns = format_suffix_patterns(urlpatterns)
