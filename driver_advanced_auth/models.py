from __future__ import unicode_literals

from django.db import models

# Create your models here.

from grout.models import BoundaryPolygon, Boundary
from django.contrib.auth.models import User, Group

IS_APPROVED = (
    (True, True),
    (False, False),
)

ROLE_REQUESTED = (
    ('Not Requested', 'Not Requested'),
    ('Requested', 'Requested'),
    ('Accepted', 'Accepted'),
    ('Rejected', 'Rejected')
)

DRIVER_PANELS = (
    ('user_panel', 'USER PANEL'),
    ('admin_panel', 'ADMIN PANEL')
)

LANGUAGE_JSON_PATH = 'var/www/media/multi_language'


class City(models.Model):
    id = models.AutoField(primary_key=True)
    country = models.ForeignKey(Boundary, blank=True, null=True, on_delete=models.CASCADE)
    region = models.ForeignKey(BoundaryPolygon, blank=True, null=True, on_delete=models.CASCADE)
    name = models.CharField(max_length=30)

    def __str__(self):
        return self.name


class Organization(models.Model):
    id = models.AutoField(primary_key=True)
    country = models.ForeignKey(Boundary, blank=True, null=True, on_delete=models.CASCADE)
    region = models.ForeignKey(BoundaryPolygon, blank=True, null=True, on_delete=models.CASCADE)
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.name


class UserDetail(models.Model):
    # user_id = models.AutoField(primary_key=True)
    first_name = models.CharField(max_length=254, blank=True, null=True)
    last_name = models.CharField(max_length=254, blank=True, null=True)
    email = models.EmailField()
    username = models.CharField(max_length=254)
    password = models.CharField(max_length=254, blank=True, )
    mobile_no = models.CharField(max_length=15, null=True, blank=True)
    # country = models.ForeignKey(Boundary, on_delete=models.CASCADE, null=True, blank=True)
    reg = models.ManyToManyField(BoundaryPolygon, blank=True, related_name='User_Region')
    city = models.ManyToManyField(BoundaryPolygon, blank=True, related_name='User_City')
    org = models.ManyToManyField(Organization, blank=True)
    groups = models.ManyToManyField(Group)
    is_active = models.BooleanField(default=True)
    date_joined = models.DateTimeField(auto_now_add=True)
    updated_on = models.DateTimeField(auto_now=True, blank=True, null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    is_staff = models.BooleanField(choices=IS_APPROVED, default=False)
    is_superuser = models.BooleanField(choices=IS_APPROVED, default=False)
    is_role_requested = models.CharField(max_length=30, choices=ROLE_REQUESTED, default='Not Requested')
    geography = models.CharField(max_length=150, null=True, blank=True)
    is_analyst = models.BooleanField(choices=IS_APPROVED, default=False)
    is_tech_analyst = models.BooleanField(choices=IS_APPROVED, default=False)
    google_user = models.BooleanField(default=False)
    password_update_hash_key = models.CharField(max_length=128, null=True, blank=True)
    password_update_hash_created_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return self.username


class Groupdetail(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=254)
    group = models.ForeignKey(Group, null=True, blank=True, on_delete=models.CASCADE)
    description = models.CharField(max_length=254, null=True, blank=True, )
    is_admin = models.BooleanField(choices=IS_APPROVED, default=False)


class SendRoleRequest(models.Model):
    # id = models.AutoField(primary_key=True)
    reg = models.ManyToManyField(BoundaryPolygon, blank=True, related_name='Region')
    country = models.ForeignKey(Boundary, blank=True, null=True, on_delete=models.CASCADE)
    user = models.ForeignKey(User, null=True, blank=True, on_delete=models.CASCADE)
    group = models.ForeignKey(Group, null=True, blank=True, on_delete=models.CASCADE)
    current_group = models.CharField(null=True, blank=True, max_length=100)
    city = models.ManyToManyField(BoundaryPolygon, blank=True, related_name='City')
    org = models.ManyToManyField(Organization, blank=True)
    is_approved = models.BooleanField(choices=IS_APPROVED, default=False)

    def __str__(self):
        return self.user.first_name + ' ' + self.user.last_name


class CountryInfo(models.Model):
    country_code = models.CharField(max_length=5, null=False, blank=False)
    country_name = models.CharField(max_length=50, null=False, blank=False)
    archived = models.BooleanField(default=True)
    latitude = models.FloatField(blank=True, null=True)
    longitude = models.FloatField(blank=True, null=True)

    def __str__(self):
        return self.country_name


class LanguageDetail(models.Model):
    label = models.CharField(max_length=50, null=True, blank=True)
    csv_file = models.CharField(max_length=100, null=True, blank=True)
    json_file = models.CharField(max_length=100, null=True, blank=True)
    language_code = models.CharField(max_length=10, null=True, blank=True)
    upload_for = models.CharField(max_length=15, choices=DRIVER_PANELS, null=True, blank=True)
    default_for_user_panel = models.BooleanField(default=False)
    default_for_admin_panel = models.BooleanField(default=False)
    archive = models.BooleanField(default=False)
    created_date = models.DateTimeField(auto_now_add=True)
    updated_date = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.language_code


class PasswordHistory(models.Model):
    id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey(User, on_delete=models.CASCADE)
    hashed_password = models.CharField(max_length=128)
    created_on = models.DateTimeField(auto_now_add=True)
