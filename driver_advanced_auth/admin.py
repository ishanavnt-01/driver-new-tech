# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin
from .models import City, Organization, UserDetail, Groupdetail, SendRoleRequest, CountryInfo
from data.models import KeyDetail, WeatherDataList
from django.forms import ModelForm
from django.contrib.postgres.fields import HStoreField


class WeatherAdmin(admin.ModelAdmin):
    list_display = ['label', 'value', 'active']


class KeyDetailAdmin(admin.ModelAdmin):
    list_display = ['keyname', 'value']


class GroupDetailAdmin(admin.ModelAdmin):
    list_display = ['name', 'description', 'is_admin']


class UserDetailAdmin(admin.ModelAdmin):
    list_display = ['username', 'email', 'is_active']


class CityAdmin(admin.ModelAdmin):
    list_display = ['name']


class OrganizationAdmin(admin.ModelAdmin):
    list_display = ['name']


# Register your models here.
admin.site.register(City, CityAdmin)
admin.site.register(Organization, OrganizationAdmin)
admin.site.register(UserDetail, UserDetailAdmin)
admin.site.register(Groupdetail, GroupDetailAdmin)
admin.site.register(SendRoleRequest)
admin.site.register(KeyDetail, KeyDetailAdmin)
admin.site.register(CountryInfo)
admin.site.register(WeatherDataList, WeatherAdmin)
