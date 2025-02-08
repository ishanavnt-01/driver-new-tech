from django.urls import re_path
from . import views

urlpatterns = [
    re_path('bindfilters/(?P<uuids>[^/]+)/$', views.BindVehicalFilterViewSet.as_view()),
    re_path('bindincidentdetailsfilters/(?P<uuids>[^/]+)/$', views.BindIncidentDetailFilterViewSet.as_view()),
    re_path(r'intervention-detail-type/', views.InterventionDetailType.as_view())
]
