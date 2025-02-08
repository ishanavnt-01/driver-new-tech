import json
from os import path as ospath
from requests import post as postrequest
from utility.response_utils import ok_response, error_response
import csv
from pandas import DataFrame as pddf


def democsv_sample(**args_dict):
    record_type = args_dict["record_type"]
    data = {"record_type_id": record_type}
    headers = {'Content-Type': 'application/json'}
    usertoken = args_dict["usertoken"]
    protocol = args_dict["protocol"] + "//"

    token_dict = {'Authorization': usertoken}
    token_dict.update(headers)
    data = json.dumps(data)
    api_url = str(protocol) + ospath.join(args_dict["apiurl"], "data-api/latestrecordschema/")

    response = postrequest(api_url, data=data, headers=token_dict)
    if not response.json()["result"]:
        return error_response(message="Schema not found")

    res_data = response.json()["result"][0]
    data = res_data["schema"]["definitions"]["driverIncidentDetails"]
    data_properties = data["properties"]

    incident_json = {}
    # anotherdict = copy.deepcopy(incident_json)
    anotherdict = {}

    valuestring = ""
    anotherdictvalue = ""

    for i in data_properties:
        if i == "_localId":
            pass
        else:
            inci_data = data_properties[i]
            if "items" in inci_data:
                if "enum" in inci_data["items"]:
                    emumdata = inci_data["items"]["enum"]
                    if type(emumdata) == list:
                        if len(emumdata) > 2:
                            valuestring = str(emumdata[0]) + "|" + str(emumdata[1])
                            anotherdictvalue = str(emumdata[0])
                        else:
                            valuestring = str(emumdata[0])
                            anotherdictvalue = str(emumdata[0])

            elif "enum" in inci_data:
                if 'enum' in inci_data:
                    emumdata = inci_data["enum"]
                    if len(emumdata) > 2:
                        valuestring = str(emumdata[0]) + "|" + str(emumdata[1])
                        anotherdictvalue = str(emumdata[0])
                    else:
                        valuestring = str(emumdata[0])
                        anotherdictvalue = str(emumdata[0])

            incident_json[str(i)] = valuestring
            anotherdict[str(i)] = anotherdictvalue

    keys_for_cols = incident_json.keys()

    # csv_columns = [i for i in keys_for_cols]
    # columnsfromdb = ['occurred_from', 'occurred_to', 'lat', 'lon', 'location_text', 'weather', 'light',
    #                  'city', 'city_district', 'county', 'neighborhood', 'road', 'state']

    staticdata = {"occurred_from": "2020-09-14 04:38", "occurred_to": "2020-09-14 04:38",
                  "lat": 17.9369530447, "lon": 102.65625,
                  "location_text": "Muangnoy, Vientiane Capital, Sisattanak District, Vientiane Prefecture, 3617, Laos",
                  "weather": "scattered clouds", "light": "Clouds", "city": "",
                  "city_district": "", "county": "", "neighborhood": "", "road": "", "state": ""}

    # allcol = csv_columns+columnsfromdb
    for datakey, dataval in staticdata.items():
        incident_json[str(datakey)] = dataval
        anotherdict[str(datakey)] = dataval

    dict_data = [incident_json, anotherdict]

    filepath = args_dict["csvfile"]
    # fullpath = str(protocol) + args_dict["apiurl"].split(":")[0] + ":3200" + "/" + str(ospath.split(filepath)[1])
    fullpath = str(protocol) + args_dict["apiurl"] + '/download/' + str(ospath.split(filepath)[1])

    df = pddf(dict_data)
    df.to_csv(filepath)

    return ok_response(data={"csvfile": fullpath})


def file_for_intervention(**args_dict):
    record_type = args_dict["record_type"]
    data = {"record_type_id": record_type}
    headers = {'Content-Type': 'application/json'}
    usertoken = args_dict["usertoken"]
    protocol = args_dict["protocol"] + "//"

    token_dict = {'Authorization': usertoken}
    token_dict.update(headers)
    data = json.dumps(data)
    api_url = str(protocol) + ospath.join(args_dict["apiurl"], "data-api/latestrecordschema/")

    response = postrequest(api_url, data=data, headers=token_dict)
    if not response.json()["result"]:
        return error_response(message="Schema not found")

    res_data = response.json()["result"][0]
    data = res_data["schema"]["definitions"]["driverInterventionDetails"]
    data_required = data["required"]
    if "_localId" in data_required:
        data_required.remove("_localId")

    data_properties = data["properties"]
    datalist = []
    for i in data_required:
        data = data_properties[i]
        for ditem in data["enum"]:
            if not ditem.isdigit():
                datalist.append(ditem)
                break

    if not datalist:
        for i in data_required:
            datalist.append('')

    for i in range(len(data_required)):
        data_required[i] = data_required[i].lower()

    data_required.extend(["lat", "lon", "occurred_from", "occurred_to"])
    datalist.extend(["120.9678160408", "14.5953366418", "2020-09-14 04:38", "2020-09-14 04:38"])

    filepath = args_dict["csvfile"]
    # fullpath = str(protocol) + args_dict["apiurl"].split(":")[0] + ":3200" + "/" + str(ospath.split(filepath)[1])
    fullpath = str(protocol) + args_dict["apiurl"] + '/download/' + str(ospath.split(filepath)[1])

    df = pddf([datalist], columns=data_required)
    df.to_csv(filepath, index=0)

    return ok_response(data={"csvfile": fullpath})
