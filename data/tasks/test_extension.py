from django.db import connections

def create_extension():
    with connections['default'].cursor() as cursor:
        cursor.execute("CREATE EXTENSION hstore;")
