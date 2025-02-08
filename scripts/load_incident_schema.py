#!/usr/bin/env python

"""Loads incidents from multiple incident database dumps (schema v3)"""
import logging
import json
import requests
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


def create_schema(schema_path, api, headers=None):
    """Create a recordtype/schema into which to load all new objects"""
    # Create record type
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
    with open(schema_path, 'r') as schema_file:
        schema_json = json.load(schema_file)
        response = requests.post(api + '/recordschemas/',
                                 data=json.dumps({u'record_type': rectype_id,
                                                  u'schema': schema_json}),
                                 headers=dict(list({'content-type': 'application/json'}.items()) +
                                              list(headers.items())))
    logger.debug(response.json())
    response.raise_for_status()
    logger.info('Created RecordSchema')
    return response.json()['uuid']


def main():
    parser = argparse.ArgumentParser(description='Load incidents data (v3)')
    parser.add_argument('--schema-path', help='Path to JSON file defining schema',
                        default='scripts/incident_schema_v3.json')
    parser.add_argument('--api-url', help='API host / path to target for loading data',
                        default='http://127.0.0.1/api')
    parser.add_argument('--authz', help='Authorization header',
                        default='Token 36df3ade778ca4fcf66ba998506bdefa54fdff1c')
    args = parser.parse_args()

    headers = None

    if args.authz:
        headers = {'Authorization': args.authz}

    if args.authz:
        headers = {'Authorization': args.authz}

    create_schema(args.schema_path, args.api_url, headers)


if __name__ == '__main__':
    main()
