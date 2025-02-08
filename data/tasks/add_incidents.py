#!/usr/bin/env python

"""Loads incidents from multiple incident database dumps (schema v3)"""
from django.conf import settings
import csv
from dateutil import parser
import glob
import logging
import json
import pytz
from time import sleep
import uuid
import os
import requests
from data.models import BulkUploadDetail, DriverRecordCopy

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


def extract(csv_path):
    """Simply pulls rows into a DictReader"""
    with open(csv_path) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        for row in reader:
            yield row


def transform(record, schema_id, required_fields):
    """Converts denormalized rows into objects compliant with the schema.

    Doesn't do anything fancy -- if the schema changes, this needs to change too.
    """
    details_mapping = []
    excepted_keys = ["occurred_from", "occurred_to", "lat", "lon", "modified", "weather", "light", "location_text"]

    for i in list(record):
        if i not in excepted_keys:
            if record[i] == "":
                if i not in required_fields:
                    del record[i]
            else:
                if i == "Severity":
                    if "|" in record[i]:
                        record[i] = record[i].split("|")
                    else:
                        record[i] = [record[i]]
                if i == "Location Approximate":
                    if type(record[i]) != list:
                        record[i] = [record[i]]

                datatype = type(record[i])
                details_mapping.append((str(i), str(i), datatype))

    # Calculate value for the occurred_from/to fields in local time
    occurred_from_date = parser.parse(record['occurred_from'])
    occurred_from_date = pytz.timezone('Asia/Manila').localize(occurred_from_date)

    occurred_to_date = parser.parse(record['occurred_to'])
    occurred_to_date = pytz.timezone('Asia/Manila').localize(occurred_to_date)
    location_text = record['location_text']

    # Set the geom field
    geomdata = "{0} ({1} {2})".format('POINT', record['lon'], record['lat'])
    weather = record['weather']
    light = record['light']
    driver_incident_dict = {}
    for csv_key, driver_key, cast_func in details_mapping:
        if csv_key in record:
            if cast_func == str:
                driver_incident_dict[driver_key] = cast_func(record[csv_key].strip())
            else:
                driver_incident_dict[driver_key] = cast_func(record[csv_key])
            # driver_incident_dict[driver_key] = cast_func(record[csv_key])

    obj = {
        'data': {
            'driverIncidentDetails': driver_incident_dict,

            'driverPerson': [],
            'driverVehicle': []
        },
        'schema': str(schema_id),
        'occurred_from': occurred_from_date.isoformat(),
        'occurred_to': occurred_to_date.isoformat(),
        'geom': geomdata,
        'uploaded_from': "bulk_upload",
        'weather': weather,
        'light': light,
        'location_text': location_text
    }

    # Add in the _localId field; they're not used here but the schema requires them
    obj['data']['driverIncidentDetails']['_localId'] = str(uuid.uuid4())
    return obj


incorrect_records = 0


def load(obj, api, f_name, headers=None):
    """Load a transformed object into the data store via the API"""
    if headers is None:
        headers = {}

    url = api + '/records/'

    data = json.dumps(obj)
    headers = dict(headers)
    headers.setdefault('content-type', 'application/json')
    while True:
        response = requests.post(url, data=data, headers=headers)
        sleep(0.2)
        if response.status_code == 201:
            ###### Add record in DriverRecordCopy table ####
            allcolumns = [i.name for i in DriverRecordCopy._meta.get_fields()]
            allcolumns.remove("uuid")
            fkeys = ["schema", "record_ptr"]
            try:
                record_obj = DriverRecordCopy(record_id=response.json()["uuid"])
                record_obj.record_ptr_id = response.json()["uuid"]
                for i in allcolumns:
                    if i in obj:
                        if i == "record_ptr":
                            record_obj.record_ptr_id = response.json()["uuid"]
                        elif i == "schema":
                            # record_obj.__schema=obj["schema"]
                            record_obj.schema_id = obj["schema"]
                        else:
                            try:
                                record_obj.__dict__[i] = obj[i]
                            except:
                                pass
                    else:
                        pass
                record_obj.save()

            except:
                pass
                ####### End of Add record in DriverRecordCopy table #############
            return
        else:
            global incorrect_records
            incorrect_records += 1
            file_name = f_name.split('/')[-1]
            bulk_upload_obj = BulkUploadDetail.objects.get(file_name=file_name)
            bulk_upload_obj.file_status = 'ERROR'
            bulk_upload_obj.save()
            logger.error(response.text)
            logger.error('retrying...')
            break


def create_schema(schema_path, api, headers=None):
    """Create a recordtype/schema into which to load all new objects"""
    # Create record type
    response = requests.get(api + '/recordtypes/?label=Incident&active=True',
                            headers=headers)
    response.raise_for_status()
    results = response.json()
    try:
        for i in results:
            if i['label'] == "Incident":
                rectype_id = i['uuid']

        # rectype_id = results[0]['uuid']
        logger.info('Loaded RecordType')
    except IndexError:
        response = requests.post(api + '/recordtypes/',
                                 data={'label': 'Incident',
                                       'plural_label': 'Incidents',
                                       'description': 'Historical incident data',
                                       'temporal': True,
                                       'active': True},
                                 headers=headers)
        response.raise_for_status()
        rectype_id = response.json()['uuid']
        logger.info('Created RecordType')

    # Create associated schema

    new_headers = {'content-type': 'application/json'}
    new_headers.update(headers)
    apiname = api.split("/api")[0]
    data = json.dumps({"record_type_id": rectype_id})

    response = requests.post(apiname + "/data-api/latestrecordschema/",
                             data=data,
                             headers=new_headers)

    scmaid = ""
    if "uuid" in response.json()["result"][0]:
        scmaid = response.json()["result"][0]["uuid"]
        logger.debug(response.json())
        response.raise_for_status()
        logger.info('RecordSchema Loaded')
    else:
        with open(schema_path, 'r') as schema_file:
            schema_json = json.load(schema_file)
            response = requests.post(api + '/recordschemas/',
                                     data=json.dumps({u'record_type': rectype_id,
                                                      u'schema': schema_json}),
                                     # headers=dict({'content-type': 'application/json'}.items() +
                                     #              headers.items()))
                                     headers=new_headers)
            scmaid = response.json()['uuid']
        logger.debug(response.json())
        response.raise_for_status()
        logger.info('Created RecordSchema')
    return scmaid, rectype_id


# @periodic_task(run_every=(crontab(minute='*/1')), name='add_incidents')
def add_incidents():
    host_url = settings.HOST_URL
    api_url = str(host_url) + '/api'

    headers = {'Authorization': 'Token 36df3ade778ca4fcf66ba998506bdefa54fdff1c'}
    schema_path = os.path.join(os.getcwd(), "scripts", "incident_schema_v3.json")
    # incidents_csv_dir = os.path.join("scripts", "incident_csvs")
    incidents_csv_dir = os.path.join("scripts", "incident_validated_csvs")

    # Do the work
    schema_id, record_type = create_schema(schema_path, api_url, headers)

    logger.info("Loading data")
    count = 1

    schemaapi_url = os.path.join(str(host_url) + "/data-api/latestrecordschema/")
    data = {"record_type_id": record_type}
    headers_dict = {'Content-Type': 'application/json'}
    headers_dict.update(headers)
    data = json.dumps(data)
    response = requests.post(schemaapi_url, data=data, headers=headers_dict)

    if not response.json()["result"]:
        logger.info("Schema not found")

    res_data = response.json()["result"][0]

    data = res_data["schema"]["definitions"]["driverIncidentDetails"]
    required_fields = data["required"]

    if "_localId" in required_fields:
        required_fields.remove("_localId")

    # Load all files in the directory, ordered by file size
    files = sorted(glob.glob(incidents_csv_dir + '/*.csv'), key=os.path.getsize)
    logger.info("Files to process: {}".format(files))

    # for csv_file in files[0:1]:
    for csv_file in files:
        logger.info("Loading file: {}".format(csv_file))
        rcnt = 0
        for record in extract(csv_file):
            if count % 100 == 0:
                logger.info("{0} (file {1} of {2})".format(
                    count, files.index(csv_file) + 1, len(files)))
            load(transform(record, schema_id, required_fields), api_url, csv_file, headers)
            count += 1
            rcnt += 1

        if incorrect_records == 0:
            file_name = csv_file.split('/')[-1]
            bulk_upload_obj = BulkUploadDetail.objects.get(file_name=file_name)
            bulk_upload_obj.file_status = 'COMPLETED'
            bulk_upload_obj.save()
        os.unlink(csv_file)
        logger.info("{} is deleted".format(csv_file))
        logger.info("count {}".format(rcnt))
    logger.info('Loading complete')
