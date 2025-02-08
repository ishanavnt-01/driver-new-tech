"""Loads mock interventions data"""
import argparse
from datetime import datetime
import logging
import geojson
import json
import requests
import sys
import os
import uuid
from django.conf import settings
if not settings.configured:
    settings.configure(
        API_HOST=os.environ.get('API_HOST')
    )
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


def read_interventions(input_path):
    """Reads the input geojson file of interventions"""
    with open(input_path) as geojson_file:
        try:
            interventions_geojson = geojson.load(geojson_file)
        except Exception:
            logger.exception('Error parsing interventions GeoJSON file:')
            sys.exit()

        interventions = interventions_geojson['features']
        logger.info('number of interventions to load: {}'.format(len(interventions)))

        # If there are no interventions to load from the file, exit the application
        if not len(interventions):
            logger.info('no interventions to load, exiting')
            sys.exit()

        return interventions


def load(record, api, headers=None):
    """Load an object into the data store via the API"""
    if headers is None:
        headers = {}
    response = requests.post(api + '/records/',
                             data=json.dumps(record),
                             headers=dict(list({'content-type': 'application/json'}.items()) +
                                          list(headers.items())))
    print ('response------------',response)                                     
    if response.status_code != 201:
        logger.error(response.text)
    # else
    logger.error(response.text)
    # response.raise_for_status()


def transform(record, schema_id):
    """Converts records into objects compliant with the schema.

    Doesn't do anything fancy -- if the schema changes, this needs to change too.
    """

    obj = {
        'data': {
            'interventionDetails': dict(),
        },
        'schema': str(schema_id),
        'occurred_from': 'None',
        'occurred_to': 'None',
        'geom': 'POINT (0 0)'
    }

    data = obj['data']
    data['interventionDetails']['Type'] = record['properties']['Type']

    # Add in the _localId field; they're not used here but the schema requires them
    def _add_local_id(dictionary):
        dictionary['_localId'] = str(uuid.uuid4())

    _add_local_id(data['interventionDetails'])

    # Set the occurred_from/to fields
    # TODO: change from temporarily saving current time
    obj['occurred_from'] = datetime.now().isoformat()
    obj['occurred_to'] = datetime.now().isoformat()

    # Set the geom field
    obj['geom'] = record['geometry']

    return obj


def create_schema(schema_path, api, headers=None):
    """Create a recordtype/schema into which to load all new objects"""
    response = requests.get('http://'+settings.API_HOST+'/data-api' + '/recordtypes/?label=Intervention&active=True', headers=headers)
    response.raise_for_status()
    results = response.json()
    try:
        for i in results:
            if i['label'] == "Intervention":
                rectype_id = i['uuid']

        # rectype_id = results[0]['uuid']
        logger.info('Loaded RecordType')
    except IndexError:
        # Create record type
        response = requests.post(api + '/recordtypes/',
                                 data={'label': 'Intervention',
                                       'plural_label': 'Interventions',
                                       'description': 'Actions to improve traffic safety',
                                       'active': True , 
                                       'temporal' : True},
                                 headers=headers)
        response.raise_for_status()
        rectype_id = response.json()['uuid']
        # rectype_id = "3b7f0404-eb1e-438f-a57c-2253f5532a83"
        logger.info('Created RecordType')

    # Create associated schema
    with open(schema_path, 'r') as schema_file:
        schema_json = json.load(schema_file)
        response = requests.post(api + '/recordschemas/',
                                 # data=json.dumps({u'record_type': "4782d51f-6ed2-4c43-960c-2bd9912cdd25",
                                 # data=json.dumps({u'record_type': "1cf99547-a55a-4126-be5b-ecde3291c8de",
                                 data=json.dumps({u'record_type': rectype_id,
                                                  u'schema': schema_json}),
                                 headers=dict(list({'content-type': 'application/json'}.items()) +
                                              list(headers.items())))
    print("Response------" , response)
    # import ipdb;ipdb.set_trace()
    logger.warning('Schema creation response: %s', response.json())
    response.raise_for_status()
    logger.info('Created RecordSchema')
    return response.json()['uuid']


def main(**args_dict):
    """Main entry point for the script"""
    api_url = "http://" + os.path.join(args_dict["apiurl"], "api")
    headers = args_dict["header"]
    geojson_input_path = args_dict["jsonpath"]

    schema_path = os.path.join(os.getcwd(), "scripts", "interventions_schema.json")
    print("schema_pathschema_pathschema_path", schema_path, os.path.exists(schema_path))
    # Do the work
    schema_id = create_schema(schema_path, api_url, headers)
    logger.info("Loading data")
    interventions = read_interventions(geojson_input_path)
    for intervention in interventions:
        logger.warning(intervention)
        load(transform(intervention, schema_id), api_url, headers)

    logger.info('Loading interventions complete')


if __name__ == '__main__':
    main()
