"""Loads mock black spot data"""
from django.conf import settings
import os
from datetime import datetime
import glob
import logging
import json
import requests
import sys
from django.contrib.sites.models import Site
from celery import shared_task
from datetime import timedelta
from data.models import BulkUploadDetail

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


def get_record_type(api_url, headers, label):
    """Gets the record type"""
    response = requests.get('{}/recordtypes/?active=True&limit=all'.format(api_url),
                            headers=headers)
    response.raise_for_status()
    results = response.json()
    rectype_id = [rt['uuid'] for rt in results if rt['label'] == label][0]
    logger.info('record type id: {}'.format(rectype_id))
    return rectype_id


def read_black_spots(input_path):
    """Reads the input json file of black spots"""
    # import ipdb;ipdb.set_trace()
    with open(input_path) as json_file:
        try:
            black_spots_json = json.load(json_file)
        except Exception:
            logger.exception('Error parsing black spots JSON file:')
            os.unlink(input_path)
            logger.info("{} is deleted".format(input_path.split('/')[-1]))
            sys.exit()

        black_spots = black_spots_json['results']
        logger.info('number of black spots to load: {}'.format(len(black_spots)))

        # If there are no black spots to load from the file, exit the application
        if not len(black_spots):
            logger.info('no black spots to load, exiting')
            os.unlink(input_path)
            logger.info("{} is deleted".format(input_path.split('/')[-1]))
            sys.exit()

        return black_spots


def deactivate_black_spot_sets(api_url, headers, rectype_id, now):
    """Deactivates the effective black spots if there are any"""
    url = '{}/blackspotsets/?record_type={}effective_at={}'.format(api_url, rectype_id, now)
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    results = response.json()['results']
    if len(results):
        for bss in results:
            logger.info('deactivating black spot set: {}'.format(bss['uuid']))
            bss['effective_end'] = now
            response = requests.patch('{}/blackspotsets/{}/'.format(api_url, bss['uuid']),
                                      data=bss,
                                      headers=headers)
            response.raise_for_status()


def create_black_spot_set(api_url, headers, rectype_id, now):
    """Adds a new black spot set to hold the new black spots"""
    new_bss = {
        'effective_start': now,
        'record_type': rectype_id
    }
    response = requests.post('{}/blackspotsets/'.format(api_url),
                             data=json.dumps(new_bss),
                             headers=headers)
    # response.raise_for_status()
    new_bss_id = response.json()['uuid']
    logger.info('created new black spot set: {}'.format(new_bss_id))
    return new_bss_id


def create_black_spots(api_url, headers, black_spots, new_bss_id, file):
    BulkUploadDetail.objects.create(file_name=file.split('/')[-1],
                                    file_status='PENDING',
                                    record_type='Black Spots')
    """Adds the black spots"""
    for black_spot in black_spots:
        black_spot['black_spot_set'] = new_bss_id
        response = requests.post('{}/blackspots/'.format(api_url),
                                 data=json.dumps(black_spot),
                                 headers=headers)
        response.raise_for_status()
        new_bs_id = response.json()['uuid']
        logger.info('created new black spot: {}'.format(new_bs_id))


@shared_task(name="load_black_spots")
def load_black_spots():
    """Main entry point for the script"""

    api_url = str(settings.HOST_URL) + '/api'
    headers = {'Authorization': 'Token 36df3ade778ca4fcf66ba998506bdefa54fdff1c'}
    record_type_label = "Incident"
    # json_input_path = os.path.join("scripts", "black_spots_json")
    # json_input_path = args_dict["jsonpath"]
    headers_for_post = {'content-type': 'application/json',
                        'Authorization': 'Token 36df3ade778ca4fcf66ba998506bdefa54fdff1c'
                        }
    now = datetime.now().isoformat() + 'Z'
    json_folder_path = os.path.join(os.getcwd(), "scripts", 'black_spots' + "_json")

    rectype_id = get_record_type(api_url, headers, record_type_label)

    files = sorted(glob.glob(json_folder_path + '/*.json'), key=os.path.getsize)
    files_to_process = [file.split('/')[-1] for file in files]
    logger.info("Files to process: {}".format(files_to_process))

    files_list = [f_obj for f_obj in files]

    for file in files:
        black_spots = read_black_spots(file)

        # deactivate_black_spot_sets(args.api_url, headers, rectype_id, now)
        new_bss_id = create_black_spot_set(api_url, headers_for_post, rectype_id, now)
        create_black_spots(api_url, headers_for_post, black_spots, new_bss_id, file)
        logger.info('Loading black spots complete')
        os.unlink(file)
        logger.info("{} is deleted".format(file.split('/')[-1]))
        completed_file_obj = BulkUploadDetail.objects.get(file_name=file.split('/')[-1])
        completed_file_obj.file_status = 'COMPLETED'
        completed_file_obj.save()
