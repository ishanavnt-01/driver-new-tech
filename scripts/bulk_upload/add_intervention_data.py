import os
import logging
import requests
import json
import uuid
from datetime import datetime
from csv import DictReader as csvDictReader
from utility.response_utils import ok_response, error_response
from pandas import DataFrame as pdDataFrame
from data.models import IrapDetail, BulkUploadDetail

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


def transform(record, schema_id, api, headers):
    obj = {
        'data': {
            'driverInterventionDetails': dict(),
        },
        'schema': str(schema_id),
        'occurred_from': 'None',
        'occurred_to': 'None',
        'geom': 'POINT (0 0)',
        'uploaded_from': "bulk_upload"
    }

    data = obj['data']
    intervention_detail_json = requests.post(os.path.split(api)[0] + "/filter-api/intervention-detail-type/",
                                             data={"intervention_type": record["type"]},
                                             headers=headers)

    if intervention_detail_json.json():
        data['driverInterventionDetails']['Type'] = str(intervention_detail_json.json()["intervention_detail"])
    else:
        data['driverInterventionDetails']['Type'] = str(record["type"])

        # Add in the _localId field; they're not used here but the schema requires them

    def _add_local_id(dictionary):
        dictionary['_localId'] = str(uuid.uuid4())

    _add_local_id(data['driverInterventionDetails'])

    # Set the occurred_from/to fields
    # TODO: change from temporarily saving current time
    obj['occurred_from'] = datetime.now().isoformat()
    obj['occurred_to'] = datetime.now().isoformat()

    # Set the geom field
    geomdata = "{0} ({1} {2})".format('POINT', record['lon'], record['lat'])
    obj['geom'] = geomdata

    return obj


def load(record, api, headers=None):
    """Load an object into the data store via the API"""
    if headers is None:
        headers = {}

    response = requests.post(api + '/records/',
                             data=json.dumps(record),
                             headers=dict(list({'content-type': 'application/json'}.items()) +
                                          list(headers.items())))

    if response.status_code != 201:
        logger.error(response.text)
    # else
    logger.error(response.text)


def create_schema(schema_path, api, headers=None):
    """Create a recordtype/schema into which to load all new objects"""

    response = requests.get(os.path.split(api)[0] + '/data-api' + '/recordtypes/?label=Intervention&active=True',
                            headers=headers)
    response.raise_for_status()
    results = response.json()
    try:
        for i in results:
            if i['label'] == "Intervention":
                rectype_id = i['uuid']

        logger.info('Loaded RecordType')
    except IndexError:
        # Create record type
        response = requests.post(api + '/recordtypes/',
                                 data={'label': 'Intervention',
                                       'plural_label': 'Interventions',
                                       'description': 'Actions to improve traffic safety',
                                       'active': True,
                                       'temporal': True},
                                 headers=headers)
        response.raise_for_status()
        rectype_id = response.json()['uuid']
        # rectype_id = "3b7f0404-eb1e-438f-a57c-2253f5532a83"
        logger.info('Created RecordType')

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
        # Create associated schema
        with open(schema_path, 'r') as schema_file:
            schema_json = json.load(schema_file)
            response = requests.post(api + '/recordschemas/',
                                     data=json.dumps({u'record_type': rectype_id,
                                                      u'schema': schema_json}),
                                     headers=dict(list({'content-type': 'application/json'}.items()) +
                                                  list(headers.items())))
            scmaid = response.json()['uuid']
        logger.warning('Schema creation response: %s', response.json())
        response.raise_for_status()
        logger.info('Created RecordSchema')
    return scmaid


def add_interventions(**kwargs):
    csvpath = kwargs["csvpath"]
    headers = kwargs["header"]
    record_type = kwargs["record_type"]
    logfilepath = kwargs["logfilepath"]
    whole_path = kwargs["whole_path"]
    returndata = {"logfile": whole_path}
    protocol = kwargs["protocol"] + "//"

    BulkUploadDetail.objects.create(file_name=csvpath.split('/')[-1],
                                    file_status='PENDING',
                                    record_type='Intervention')

    api_url = str(protocol) + os.path.join(kwargs["apiurl"], "api")
    schema_path = os.path.join(os.getcwd(), "scripts", "interventions_schema.json")
    reader = csvDictReader(open(csvpath, newline=''))

    #################### Validation ##############################
    schema_api = str(protocol) + os.path.join(kwargs["apiurl"], "data-api/latestrecordschema/")
    data = {"record_type_id": record_type}
    headers_json = {'Content-Type': 'application/json'}
    headers_json.update(headers)
    data = json.dumps(data)
    response = requests.post(schema_api, data=data, headers=headers_json)
    if not response.json()["result"]:
        return error_response(message="Schema not found")

    res_data = response.json()["result"][0]
    datares = res_data["schema"]["definitions"]["driverInterventionDetails"]
    interventions = datares["properties"]["Type"]["enum"]
    finaldata_list = []
    for i in IrapDetail.objects.all():
        finaldata_list.append(i.irap_treatment_name)

    for i in interventions:
        if not i.isdigit():
            finaldata_list.append(i)

    invalid_data_list = []
    rowcount = 0
    for record in reader:
        rowcount += 1
        typeval = record["type"]
        if typeval.isdigit():
            msg = "In row {} the value of {} is {} which is invalid. It should be either one of these {}". \
                format(rowcount + 1, "type", typeval, finaldata_list)
            invalid_data_list.append([msg])
        else:
            intervention_detail_json = requests.post(
                str(protocol) + kwargs["apiurl"] + "/filter-api/intervention-detail-type/",
                data={"intervention_type": record["type"]},
                headers=headers)

            typeval = str(intervention_detail_json.json()["intervention_detail"])
            if typeval in interventions:
                pass
            elif typeval == "":
                msg = "In row {} the value of {} is invalid. It should be either one of these {}". \
                    format(rowcount + 1, "type", finaldata_list)
                invalid_data_list.append([msg])
            elif typeval not in finaldata_list:
                msg = "In row {} the value of {} is {} which is invalid. It should be either one of these {}". \
                    format(rowcount + 1, "type", typeval, finaldata_list)
                invalid_data_list.append([msg])

    if invalid_data_list:
        df = pdDataFrame(invalid_data_list, columns=['message'])
        df.to_excel(logfilepath, index=0)
        bulk_upload_obj = BulkUploadDetail.objects.get(file_name=csvpath.split('/')[-1])
        bulk_upload_obj.file_status = 'ERROR'
        bulk_upload_obj.save()
        os.unlink(csvpath)
        return error_response(data=returndata, message="Invalid Data in CSV")
    ################################################

    else:
        schema_id = create_schema(schema_path, api_url, headers)
        print("Create Schema Done!!!!!")
        file_reader = csvDictReader(open(csvpath, newline=''))

        for record in file_reader:
            print(record)
            load(transform(record, schema_id, api_url, headers), api_url, headers)

        logger.info('Loading Interventions Completed')
        bulk_upload_obj = BulkUploadDetail.objects.get(file_name=csvpath.split('/')[-1])
        bulk_upload_obj.file_status = 'COMPLETED'
        bulk_upload_obj.save()
        os.unlink(csvpath)
        return ok_response(message="Intervention details added Successfully")
