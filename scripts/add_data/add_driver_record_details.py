import django;django.setup()
from data.models import DriverRecord, DriverRecordCopy


def insert_driver_details():
    allcolumns = [i.name for i in DriverRecordCopy._meta.get_fields()]
    allcolumns.remove("uuid")
    allcolumns.remove("record_ptr")
    allcolumns.remove("schema")
    allcolumns.remove("record")

    for item in DriverRecord.objects.all():
        driver_obj = DriverRecordCopy(record_id=item.record_ptr_id)
        driver_obj.record_ptr_id = item.record_ptr_id
        driver_obj.schema_id = item.schema_id

        for i in allcolumns:
            try:
                driver_obj.__dict__[i] = item.__dict__[i]
            except:
                pass
        driver_obj.save()

if __name__ == "__main__":
    insert_driver_details()
    print("Done")
