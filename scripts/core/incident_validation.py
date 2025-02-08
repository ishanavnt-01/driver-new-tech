import json
import datetime
from requests import post as postrequest
from pandas import DataFrame as pdDataFrame
from csv import DictReader as csvDictReader
from os import path as ospath, getcwd as currentpath, makedirs as osmakedirs
from utility.response_utils import ok_response, error_response
from shutil import copyfile
import copy
from data.models import BulkUploadDetail
import logging

logger = logging.getLogger()


def incidentvalidation(**args_dict):
    csvpath = args_dict["uploadedcsv"]
    record_type = args_dict["record_type"]
    usertoken = args_dict["usertoken"]
    output_filename = args_dict["logfilepath"]
    returnpath = args_dict["returnpath"]
    returndata = {"logfile": returnpath}
    protocol = args_dict["protocol"] + "//"

    api_url = str(protocol) + ospath.join(args_dict["apiurl"], "data-api/latestrecordschema/")

    data = {"record_type_id": record_type}
    headers = {'Content-Type': 'application/json'}
    token_dict = {'Authorization': usertoken}

    token_dict.update(headers)
    data = json.dumps(data)
    response = postrequest(api_url, data=data, headers=token_dict)

    if not response.json()["result"]:
        return error_response(message="Schema not found")
    res_data = response.json()["result"][0]

    data = res_data["schema"]["definitions"]["driverIncidentDetails"]
    required_fields = data["required"]

    if "_localId" in required_fields:
        required_fields.remove("_localId")

    incident_json = {}
    incident_json_data = {}
    data_properties = data["properties"]
    new_final_list = copy.deepcopy(data_properties)

    for i in data_properties:
        if i == "_localId":
            pass
        else:
            inci_data = data_properties[i]
            deep_inci_data = new_final_list[i]
            if "items" in inci_data:
                if "enum" in inci_data["items"]:
                    emumdata = inci_data["items"]["enum"]
                    incident_json_data[str(i)] = deep_inci_data["items"]["enum"]
                    if type(emumdata) == list:
                        for eachitem in range(len(emumdata)):
                            emumdata[eachitem] = emumdata[eachitem].lower()
            elif "enum" in inci_data:
                emumdata = inci_data["enum"]
                incident_json_data[str(i)] = deep_inci_data["enum"]
                if type(emumdata) == list:
                    for eachitem in range(len(emumdata)):
                        emumdata[eachitem] = emumdata[eachitem].lower()
            incident_json[str(i)] = emumdata

    required_field_nflst = []
    invalid_data_list = []

    reader = csvDictReader(open(csvpath, newline=''))

    first_row = next(reader)
    mylist = list(first_row.keys())

    for i in required_fields:
        if i not in mylist:
            msg = "{0} {1}".format(i, "column is not found in csv")
            required_field_nflst.append([msg])
    if required_field_nflst:
        df = pdDataFrame(required_field_nflst, columns=['message'])
        df.to_excel(output_filename, index=0)
        return error_response(data=returndata, message="Invalid Data in CSV")
    else:
        rowcount = 1
        ####### Jugad For first Row ####
        date_format = '%Y-%m-%d %H:%M'

        for key, value in first_row.items():
            if key in ["occurred_from", "occurred_to"]:
                try:
                    date_obj = datetime.datetime.strptime(value, date_format)
                except:
                    # date_format = '%Y-%m-%d %H:%M:%S'
                    # try:
                    #     date_obj = datetime.datetime.strptime(value, date_format)
                    # except:
                    msg = "In row {} the value of {} is {} which is invalid. It should be {}". \
                        format(rowcount, key, first_row[key], 'YYYY-MM-DD HH:MM')
                    invalid_data_list.append([msg])
            else:

                if key in incident_json:
                    subvalues = value.split("|")
                    for eachitem in range(len(subvalues)):
                        subvalues[eachitem] = subvalues[eachitem].lower().strip()

                    if set(subvalues).issubset(incident_json[key]):
                        pass
                    else:
                        if first_row[key] == "":
                            if key in required_fields:
                                msg = "In row {} the value of {} is invalid. It should be in {}". \
                                    format(rowcount, key, incident_json_data[key])
                                invalid_data_list.append([msg])
                        else:
                            if first_row[key].lower() in incident_json[key]:
                                pass
                            else:
                                """Error log are HERE"""
                                if key in incident_json_data:
                                    msg = "In row {} the value of {} is {} which is invalid. It should be either one of these {}". \
                                        format(rowcount, key, first_row[key], incident_json_data[key])
                                    invalid_data_list.append([msg])
                else:
                    pass

        ####### Jugad For first Row ####

        for row in reader:
            rowcount += 1
            for key, value in row.items():
                if key in ["occurred_from", "occurred_to"]:
                    try:
                        date_obj = datetime.datetime.strptime(value, date_format)
                    except:
                        # date_format = '%Y-%m-%d %H:%M:%S'
                        # try:
                        #     date_obj = datetime.datetime.strptime(value, date_format)
                        # except:

                        msg = "In row {} the value of {} is {} which is invalid. It should be {}". \
                            format(rowcount, key, row[key], 'YYYY-MM-DD HH:MM')
                        invalid_data_list.append([msg])
                else:
                    if key in incident_json:
                        #####
                        if key == "Main Cause":
                            if "|" in value:
                                msg = "In row {} the value of {} is {} which is invalid. It accept ony One value .It should be either one of these {}". \
                                    format(rowcount, key, row[key], incident_json_data[key])
                                invalid_data_list.append([msg])
                        #####
                        else:
                            subvalues = value.split("|")
                            for eachitem in range(len(subvalues)):
                                subvalues[eachitem] = subvalues[eachitem].lower().strip()

                            if set(subvalues).issubset(incident_json[key]):
                                pass
                            else:
                                if row[key] == "":
                                    if key in required_fields:
                                        msg = "In row {} the value of {} is invalid. It should be in {}". \
                                            format(rowcount, key, incident_json_data[key])
                                        invalid_data_list.append([msg])
                                else:
                                    if row[key].lower() in incident_json[key]:
                                        pass
                                    else:
                                        """Error log are HERE"""
                                        if key in incident_json_data:
                                            msg = "In row {} the value of {} is {} which is invalid. It should be either one of these {}". \
                                                format(rowcount, key, row[key], incident_json_data[key])
                                            invalid_data_list.append([msg])
                    else:
                        pass
        if invalid_data_list:
            df = pdDataFrame(invalid_data_list, columns=['message'])
            df.to_excel(output_filename, index=0)
            return error_response(data=returndata, message="Invalid Data in CSV")
        else:
            validated_folder_path = ospath.join(currentpath(), "scripts", "incident_validated_csvs")
            if not ospath.exists(validated_folder_path):
                osmakedirs(validated_folder_path)

            val_csvpath = ospath.join(validated_folder_path, ospath.split(csvpath)[1])
            copyfile(csvpath, val_csvpath)
            file_name = csvpath.split('/')[-1]
            BulkUploadDetail.objects.create(
                file_name=file_name,
                file_status='PENDING',
                record_type='Incident'
            )
            return ok_response(message="All Data is Correct")
