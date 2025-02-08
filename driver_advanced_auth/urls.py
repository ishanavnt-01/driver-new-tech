from django.urls import re_path, path
from . import views
from data import views as data_views

urlpatterns = [
    # path('newusers/', views.UserList.as_view()),#not in use
    # re_path(r'userbyid/(?P<pk>[0-9]+)/$', views.UserById.as_view()),#not in use
    # re_path(r'groupbyid/(?P<pk>[0-9]+)/$', views.GroupById.as_view()),#not in use
    # path('city/', views.CityList.as_view()),#not in use for samoa
    # path('getregion/', views.GetRegion.as_view()),#not in use for samoa
    #
    # path('getcountry/', views.GetCountry.as_view()),#not in use
    path('org/', views.OrganizationList.as_view()),
    path('driver-group/', views.DriverGroup.as_view()),
    path('getcontentpermission/', views.GetContentTypesbyname.as_view()),
    path('getlistmodels/', views.GetContentTypes.as_view()),
    path('getpermissionbymodule/', views.permissionByContentId.as_view()),
    path('getgrouppermission/', views.GetGroupWisePermission.as_view()),
    path('requestforrole/', views.RequestForRole.as_view()),
    # new_api's
    re_path(r'get-auth-user-details-list/(?P<id>[0-9]+)/$', views.GetAuthUserListDetails.as_view()),
    re_path(r'driver-group-detail/(?P<id>[0-9]+)/$', views.DriverGroupDetail.as_view()),
    re_path(r'city-details/(?P<id>[0-9]+)/$', views.CityDetailList.as_view()),
    re_path(r'org-detail/(?P<id>[0-9]+)/$', views.OrganisationDetailList.as_view()),
    path('adv-registration/', views.AdvUserRegisterAPI.as_view()),
    re_path(r'adv-registration-detail/(?P<id>[0-9]+)/$', views.AdvUserRegisterDetailAPI.as_view()),
    re_path(r'user-info/(?P<id>[0-9]+)/$', views.UserDetails.as_view()),
    re_path(r'region-cities/(?P<uuids>[^/]+)/$', views.RegionCities.as_view()),
    re_path(r'region-organization/(?P<uuids>[^/]+)/$', views.RegionOrganization.as_view()),
    re_path(r'get-role-detail/(?P<id>[0-9]+)/$', views.RoleDetails.as_view()),
    re_path(r'accept-role-request/(?P<user>[0-9]+)/$', views.AcceptRoleRequest.as_view()),
    re_path(r'reject-role-request/(?P<user>[0-9]+)/$', views.RejectRoleRequest.as_view()),
    re_path(r'region-name/(?P<uuids>[^/]+)/$', views.RegionNames.as_view()),
    re_path(r'check-role-requested-status/(?P<id>[0-9]+)/', views.CheckRoleStatus.as_view()),
    path('getcities/', views.GetCities.as_view()),
    path('weather-info/', views.WeatherInfoDetails.as_view()),
    re_path(r'weather-info-details/(?P<id>[0-9]+)/', views.WeatherInfoDetailById.as_view()),
    path('google-login/', views.RegisterGoogleUser.as_view()),
    path('use-as-record/', data_views.UseRecord.as_view()),
    re_path(r'merged-and-updated-records/(?P<uuids>[^/]+)/$', views.MergedAndUpdatedRecords.as_view()),
    path('find-existing-records/', views.FindExisting.as_view()),
    path('get-json-key/', views.GetJsonSchemaKey.as_view()),
    re_path(r'get-intervention-type-detail/(?P<uuid>[^/]+)/$', views.intervention_type_detail),

    path('send-update-password-link/', views.send_update_password_link),
    path('validate-reset-password-url/', views.validate_reset_password_url),
    path('update-password/', views.update_password)
]
