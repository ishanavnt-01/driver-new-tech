import django;django.setup()
from django.contrib.auth.models import User
from driver_advanced_auth.models import UserDetail


def insert_user_details():
    user_objs = User.objects.all()

    for item in user_objs:
        user_id = item.id
        review, created = UserDetail.objects.get_or_create(user=item)
        user_detail_obj = UserDetail.objects.get(user=item)
        user_detail_obj.password = item.password
        user_detail_obj.username = item.username
        user_detail_obj.first_name = item.first_name
        user_detail_obj.last_name = item.last_name
        user_detail_obj.email = item.email
        user_detail_obj.save()


if __name__ == "__main__":
    insert_user_details()
    print("Done")

