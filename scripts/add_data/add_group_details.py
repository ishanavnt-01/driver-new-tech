import django;django.setup()
from django.contrib.auth.models import Group
from driver_advanced_auth.models import Groupdetail


def insert_groups():
    for item in Group.objects.all():
        review, created = Groupdetail.objects.get_or_create(group=item)
        user_group_obj = Groupdetail.objects.get(group=item)
        user_group_obj.name = item.name
        user_group_obj.save()


if __name__ == "__main__":
    insert_groups()
    print("Done")
