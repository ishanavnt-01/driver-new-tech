"""DRIVER URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import include, re_path, path
from driver_advanced_auth import views
from rest_framework import routers
from grout import views as grout_views
from black_spots import views as black_spot_views
from data import views as data_views
from user_filters import views as filter_views
from driver_irap.client import login_irap, getdataset, getlat_lon, fatalitydata
from black_spots.views import json_encode, csvsample

router = routers.DefaultRouter()

router.register('audit-log', data_views.DriverRecordAuditLogViewSet)
router.register('boundaries', grout_views.BoundaryViewSet)
router.register('boundarypolygons', grout_views.BoundaryPolygonViewSet)
router.register('csv-export', data_views.RecordCsvExportViewSet, basename='csv-export')
router.register('duplicates', data_views.DriverRecordDuplicateViewSet)
router.register('records', data_views.DriverRecordViewSet)
router.register('getrecords', data_views.DriverGetRecordViewSet, basename='driver-get-records')
router.register('recordschemas', data_views.DriverRecordSchemaViewSet)
router.register('recordtypes', data_views.DriverRecordTypeViewSet, basename='data')
router.register('recordcosts', data_views.DriverRecordCostConfigViewSet)
router.register('assignments', black_spot_views.EnforcerAssignmentViewSet)
router.register('blackspots', black_spot_views.BlackSpotViewSet, basename='blackspots')
router.register('blackspotsets', black_spot_views.BlackSpotSetViewSet, basename='blackspotsets')
router.register('blackspotconfig', black_spot_views.BlackSpotConfigViewSet, basename='blackspotconfig')
router.register('userfilters', filter_views.SavedFilterViewSet, basename='userfilters')
router.register('dedupe-config', data_views.DedupeDistanceConfigViewSet, basename='dedupe-config')
router.register('country-info', views.CountryDetails, basename='country-info')
router.register('language-details', views.LanguageDetailsViewSet, basename='language-details')
router.register('bulk-upload-details', data_views.BulkUploadDetailViewSet, basename='bulk-upload-details')
router.register('map-details', data_views.RecordMapDetais, basename='map-details')
router.register('weather-data-list', data_views.WeatherDataListViewset, basename='weather-data-list')
router.register('key-detail-list', data_views.KeyDetailsViewSet, basename='key-detail-list')

urlpatterns = [
    path('admin/', admin.site.urls),
    path('auth-api/', include('driver_advanced_auth.urls')),
    path('filter-api/', include('user_filters.urls')),
    path('data-api/', include('data.urls')),
    path('api/', include(router.urls)),
    path('api-token-auth/', views.obtain_auth_token),
    path('api/sso-token-auth/', views.sso_auth_token),
    path('api/registration/', views.UserRegistrationAPI.as_view()),
    path('api/auth-group/', views.GroupsList.as_view()),
    path('api/record-copy/', views.RecordCopy.as_view()),

    re_path(r'api/registration-detail/(?P<id>[0-9]+)/$', views.UserRegistrationDetailAPI.as_view()),
    re_path(r'api/auth-group-detail/(?P<id>[0-9]+)/$', views.GroupListDetail.as_view()),
    re_path(r'api/return-token/(?P<id>[0-9]+)/$', views.ReturnUserToken.as_view()),
    re_path(r'api/record-copy-details/(?P<uuid>[^/]+)/$', views.RecordCopyDetails.as_view()),
    re_path(r'api/duplicate-record-list/(?P<uuid>[^/]+)/$', data_views.DuplicateRecordDetails.as_view()),
    re_path(r'api/lang-json/(?P<lang_code>\w+)/(?P<upload_for>\w+)/$', views.returntranslatedresponsecontent),

    path('api/bulkupload/', json_encode),
    path('api/samplecsv/', csvsample),
    path('api/data-check/', views.check_required_data),

    # irap
    path('api/irap-login/', login_irap),
    path('api/irap-getdataset/', getdataset),
    path('api/irap-getlat_lon/', getlat_lon),
    path('api/irap-fatalitydata/', fatalitydata),
]
