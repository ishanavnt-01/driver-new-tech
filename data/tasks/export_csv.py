import csv
import os
import zipfile
import tempfile
import time
# from io import StringIO
import io
import pytz
import pandas as pd
from DRIVER import settings
from django.contrib.auth.models import User, Group

from celery import shared_task
from celery.utils.log import get_task_logger

from django_redis import get_redis_connection

from data.models import DriverRecord, IrapDetail

# from driver_auth.permissions import is_admin_or_writer

logger = get_task_logger(__name__)
local_tz = pytz.timezone(settings.TIME_ZONE)


def _utf8(value):
    """
    Helper for properly encoding values that may contain unicode characters.
    From https://github.com/azavea/django-queryset-csv/blob/master/djqscsv/djqscsv.py#L174

    :param value: The string to encode
    """
    if isinstance(value, str):
        return value
    # elif isinstance(value, str):
    #     return value.encode('utf-8')
    else:
        return str(value).encode('utf-8')


def _sanitize(value):
    """
    Helper for sanitizing the record type label to ensure it doesn't contain characters that are
    invalid in filenames such as slashes.
    This keeps spaces, periods, underscores, and all unicode characters.

    :param value: The string to sanitize
    """
    return ''.join(char for char in value if char.isalnum() or char in [' ', '.', '_']).rstrip()


@shared_task(track_started=True)
def export_csv(query_key, user_id, group_id):
    """Exports a set of records to a series of CSV files and places them in a compressed tarball
    :param query_key: A UUID corresponding to a cached SQL query which will be used to filter
                      which records are returned. This is the same key used to generate filtered
                      Windshaft tiles so that the CSV will correspond to the filters applied in
                      the UI.
    """
    # Get Records
    records = get_queryset_by_key(query_key)
    # Get the most recent Schema for the Records' RecordType
    # This assumes that all of the Records have the same RecordType.

    try:
        record_type = records[0].schema.record_type
        schema = record_type.get_current_schema()
    except IndexError:
        raise Exception('Filter includes no records')

    # Get user
    user = User.objects.get(pk=user_id)
    group_name = str(Group.objects.get(pk=group_id))
    # Create files and CSV Writers from Schema
    if (user and group_name != settings.Read_Only_group):
        record_writer = DriverRecordExporter(schema)
    elif (user and group_name == settings.Read_Only_group):
        record_writer = ReadOnlyRecordExporter(schema)

    # Write records to files
    for rec in records:
        record_writer.write_record(rec)

    record_writer.finish()

    # Compress files into a single zipfile.
    # TODO: Figure out how to transfer files to web users from celery workers

    # external_attr is 4 bytes ins size. The high order two bytes represend UNIX permission and
    # file type bits, while the low order two contain MS-DOS FAT file attributes, most notably
    # bit 4 marking directories
    # For information on setting file permissions in zipfile, see
    # http://stackoverflow.com/questions/434641/how-do-i-set-permissions-attributes-on-a-file-in-a-zip-file-using-pythons-zip

    filename = "{}-{}.zip".format(_utf8(_sanitize(record_type.plural_label)), query_key[:8])
    path = os.path.join(settings.CELERY_EXPORTS_FILE_PATH, filename)

    archive = zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED)
    # Add a directory for the schema we're outputting
    dirname = 'schema-' + str(schema.pk) + '/'
    for f, name in record_writer.get_files_and_names():
        t = time.struct_time(time.localtime(time.time()))
        info = zipfile.ZipInfo(filename=dirname + name, date_time=(
            t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec
        ))
        info.external_attr = 0o755 << 16
        with open(f.name, 'r') as z:
            csv_file = pd.read_csv(z)
            # f_name = '/home/shubham/DRIVER_3.8/driver_new_tech/' + name.split('.')[0] + '.xlsx'
            f_name = os.path.join(settings.CELERY_EXPORTS_FILE_PATH, name.split('.')[0] + '.xlsx')
            abs_src = os.path.abspath(settings.CELERY_EXPORTS_FILE_PATH)
            csv_file.to_excel(f_name, index=None, header=True)
            absname = f_name
            # for getting name only of a xlsx file
            arcname = absname[len(abs_src) + 1:]
            if arcname.split('.')[1] == 'xlsx':
            # for csv
            # archive.writestr(info, z.read())
                # for xlsx only with the file name not path
                archive.write(absname, arcname)
                # for removing the xlsx files
                os.unlink(f_name)
    archive.close()

    # Cleanup
    record_writer.cleanup()

    return os.path.basename(archive.filename)


def get_sql_string_by_key(key):
    """Returns a SQL string from Redis using key
    :param key: A UUID pointing to the SQL string
    """
    # Since the records list endpoint bypasses the Django caching framework, do that here too
    redis_conn = get_redis_connection('default')

    return redis_conn.get(key)


def get_queryset_by_key(key):
    """Returns a queryset by filtering Records using the SQL stored in Redis at key
    :param key: A UUID specifying the SQL string to use
    """
    sql_str = get_sql_string_by_key(key)

    #################### Query Updation ##########
    mystr = str(sql_str, 'utf-8')
    mystr = mystr.replace("%", "%%")
    mystr = mystr.replace("LIKE", "ILIKE")

    # if "UPPER" in mystr:
    #     original_string1 = mystr.split("ILIKE")[0]
    #     last_char_index = original_string1.rfind("UPPER(")
    #     new_string1 = original_string1[:last_char_index] + "" + original_string1[last_char_index + 1:]
    #     last_char_index = new_string1.rfind("P")
    #     new_string1 = new_string1[:last_char_index] + "" + new_string1[last_char_index + 1:]
    #     last_char_index = new_string1.rfind("P")
    #     new_string1 = new_string1[:last_char_index] + "" + new_string1[last_char_index + 1:]
    #     last_char_index = new_string1.rfind("E")
    #     new_string1 = new_string1[:last_char_index] + "" + new_string1[last_char_index + 1:]
    #     last_char_index = new_string1.rfind("R")
    #     new_string1 = new_string1[:last_char_index] + "" + new_string1[last_char_index + 1:]
    #     last_char_index = new_string1.rfind("(")
    #     new_string1 = new_string1[:last_char_index] + "" + new_string1[last_char_index + 1:]
    #     last_char_index = new_string1.rfind(")")
    #     new_string1 = new_string1[:last_char_index] + "" + new_string1[last_char_index + 1:]
    #
    #     if mystr.split("ILIKE")[1].strip().startswith("UPPER"):
    #         new_string2 = mystr.split("ILIKE")[1].strip().replace("UPPER(", "", 1).replace(")", "", 1)
    #     else:
    #         new_string2 = mystr.split("ILIKE")[1]
    #     mystr = new_string1 + " ILIKE " + new_string2
    #
    # left, bracket, rest = mystr.partition('"grout_record"."location_text"::')
    # block, bracket, right = rest.partition("AND")
    # address_from_user = eval(rest.split("ILIKE")[1].split("AND")[0]).replace("%", "")
    #
    # new_string = ""
    #
    # for i in address_from_user.split(" "):
    #     name = str('"%"' + i + '"%"')
    #     make_list = list(name)
    #     make_list[2] = ""
    #     make_list[-3] = ""
    #     name = ''.join(make_list)  # .replace("", "")
    #     name = name.replace('"', "'")
    #     new_string = '"grout_record"."location_text"::text ILIKE ' + name + ' OR ' + new_string
    #
    # # center_string = '"grout_record"."location_text"::text ' + new_string
    # new_sql_string = left + new_string + right
    # new_sql_string = new_sql_string.replace("%", "%%")
    #################### Query Updation ##########

    # return DriverRecord.objects.raw(str(sql_str,'utf-8').replace("%", "%%").replace("LIKE","ILIKE"))
    # return DriverRecord.objects.raw(str(mystr))

    ############ 18th Jun
    if '"grout_record"."location_text" ILIKE' in mystr:
        split_sql_str = mystr.split("%%")
        import re
        address_from_user = []
        address_user = split_sql_str[1]
        address_user = re.split(' |,', address_user)
        for i in address_user:
            if i not in address_from_user:
                address_from_user.append(i)


        new_string = ""
        for i in address_from_user[:]:
            name = str('"%"' + i + '"%"')
            make_list = list(name)
            make_list[2] = ""
            make_list[-3] = ""
            name = ''.join(make_list)  # .replace("", "")
            name = name.replace('"', "'")
            if address_from_user.index(i) == 0:
                new_string = name + ' AND ' + str(new_string)
            elif address_from_user.index(i) == len(address_from_user) - 1:
                new_string = str(new_string) + '"grout_record"."location_text"::text ILIKE ' + name
            else:
                new_string = str(new_string) + '"grout_record"."location_text"::text ILIKE ' + name + ' AND '

        new_sql_string = split_sql_str[0] + new_string + split_sql_str[2]
        new_sql_string = new_sql_string.replace("%", "%%")
        return DriverRecord.objects.raw(str(new_sql_string))
    ############ 18th Jun
    else:
        return DriverRecord.objects.raw(str(mystr))


class DriverRecordExporter(object):
    """Exports Records matching a schema to CSVs"""

    def __init__(self, schema_obj):
        # Detect related info types and set up CSV Writers as necessary
        self.schema = schema_obj.schema

        # Make output writers and output files
        self.rec_writer = self.make_record_and_details_writer()
        # All non-details related info types
        self.writers = {related: self.make_related_info_writer(related, subschema)
                        for related, subschema in self.schema['definitions'].items()
                        if not subschema.get('details')}

        self.rec_outfile, self.outfiles = self.setup_output_files()
        self.write_headers()

    def setup_output_files(self):
        """Create the output files necessary for writing CSVs"""
        # Using NamedTemporaryFiles is necessary for creating tarballs containing temp files
        # https://bugs.python.org/issue21044
        rec_outfile = tempfile.NamedTemporaryFile(delete=False, mode="wt")
        outfiles = {related: tempfile.NamedTemporaryFile(delete=False, mode="wt")
                    for related, subschema in self.schema['definitions'].items()
                    if not subschema.get('details')}
        return (rec_outfile, outfiles)

    def write_headers(self):
        """Write CSV headers to output files"""
        # Write CSV header to all files
        self.rec_writer.write_header(self.rec_outfile)
        for related_name, writer in self.writers.items():
            writer.write_header(self.outfiles[related_name])

    def finish(self):
        """Close all open file handles"""
        self.rec_outfile.close()
        for f in self.outfiles.values():
            f.close()

    def cleanup(self):
        """Deletes all temporary files"""
        os.remove(self.rec_outfile.name)
        for f in self.outfiles.values():
            os.remove(f.name)

    def get_files_and_names(self):
        """Return all file objects maintained by this exporter along with suggested names"""
        yield (self.rec_outfile, 'records.csv')
        for related_name, out_file in self.outfiles.items():
            yield (out_file, related_name + '.csv')

    def write_record(self, rec):
        """Pass rec's fields through all writers to output all info as CSVs"""
        # First the constants writer
        self.rec_writer.write_record(rec, self.rec_outfile)
        # Next, use the related info writers to output to the appropriate files
        for related_name, writer in self.writers.items():
            if related_name in rec.data:
                if writer.is_multiple:
                    for item in rec.data[related_name]:
                        writer.write_related(rec.pk, item, self.outfiles[related_name])
                else:
                    writer.write_related(rec.pk, rec.data[related_name],
                                         self.outfiles[related_name])

    def make_constants_csv_writer(self):
        """Generate a Record Writer capable of writing out the non-json fields of a Record"""

        def render_date(d):
            return d.astimezone(local_tz).strftime('%Y-%m-%d %H:%M:%S')

        # TODO: Currently this is hard-coded; it may be worthwhile to make this introspect Record
        # to figure out which fields to use, but that will be somewhat involved.
        csv_columns = ['record_id', 'timezone', 'created', 'modified', 'occurred_from',
                       'occurred_to', 'lat', 'lon', 'location_text',
                       'city', 'city_district', 'county', 'neighborhood', 'road',
                       'state', 'weather', 'light']
        # Model field from which to get data for each csv column
        source_fields = {
            'record_id': 'uuid',
            'timezone': None,
            'lat': 'geom',
            'lon': 'geom'
        }

        # Some model fields need to be transformed before they can go into a CSV
        value_transforms = {
            'record_id': lambda uuid: str(uuid),
            'timezone': lambda _: settings.TIME_ZONE,
            'created': render_date,
            'modified': render_date,
            'occurred_from': render_date,
            'occurred_to': render_date,
            'lat': lambda geom: geom.y,
            'lon': lambda geom: geom.x,
        }
        return RecordModelWriter(csv_columns, source_fields, value_transforms)

    def make_related_info_writer(self, info_name, info_definition, include_record_id=True):
        """Generate a RelatedInfoExporter capable of writing out a particular related info field
        :param info_definition: The definitions entry providing the sub-schema to write out.
        """
        # Need to drop Media fields; we can't export them to CSV usefully.
        drop_keys = dict()
        for prop, attributes in info_definition['properties'].items():
            if 'media' in attributes:
                drop_keys[prop] = None
        return RelatedInfoWriter(info_name, info_definition, field_transform=drop_keys,
                                 include_record_id=include_record_id)

    def make_record_and_details_writer(self):
        """Generate a writer to put record fields and details in one CSV"""
        model_writer = self.make_constants_csv_writer()
        details = {key: subschema for key, subschema in (self.schema['definitions'].items())
                   if subschema.get('details') is True}
        details_key = list(details.keys())[0]
        details_writer = self.make_related_info_writer(details_key, details[details_key],
                                                       include_record_id=False)
        return ModelAndDetailsWriter(model_writer, details_writer, details_key)


class DriverRecordExporterForPublic(object):
    """Exports Records matching a schema to CSVs"""

    def __init__(self, schema_obj):
        # Detect related info types and set up CSV Writers as necessary
        self.schema = schema_obj.schema

        # Make output writers and output files
        self.rec_writer = self.make_record_and_details_writer()
        # All non-details related info types
        self.writers = {related: self.make_related_info_writer(related, subschema)
                        for related, subschema in self.schema['definitions'][settings.export_csv_keyname]
                        if not subschema.get('details')}

        self.rec_outfile, self.outfiles = self.setup_output_files()
        self.write_headers()

    def setup_output_files(self):
        """Create the output files necessary for writing CSVs"""
        # Using NamedTemporaryFiles is necessary for creating tarballs containing temp files
        # https://bugs.python.org/issue21044
        rec_outfile = tempfile.NamedTemporaryFile(delete=False, mode="wt")
        outfiles = {related: tempfile.NamedTemporaryFile(delete=False, mode="wt")
                    for related, subschema in self.schema['definitions'].items()
                    if not subschema.get('details')}
        return (rec_outfile, outfiles)

    def write_headers(self):
        """Write CSV headers to output files"""
        # Write CSV header to all files
        self.rec_writer.write_header(self.rec_outfile)
        for related_name, writer in self.writers.items():
            writer.write_header(self.outfiles[related_name])

    def finish(self):
        """Close all open file handles"""
        self.rec_outfile.close()
        for f in self.outfiles.values():
            f.close()

    def cleanup(self):
        """Deletes all temporary files"""
        os.remove(self.rec_outfile.name)
        for f in self.outfiles.values():
            os.remove(f.name)

    def get_files_and_names(self):
        """Return all file objects maintained by this exporter along with suggested names"""
        yield (self.rec_outfile, 'records.csv')
        for related_name, out_file in self.outfiles.items():
            yield (out_file, related_name + '.csv')

    def write_record(self, rec):
        """Pass rec's fields through all writers to output all info as CSVs"""
        # First the constants writer
        self.rec_writer.write_record(rec, self.rec_outfile)
        # Next, use the related info writers to output to the appropriate files
        for related_name, writer in self.writers.items():
            if related_name in rec.data:
                if writer.is_multiple:
                    for item in rec.data[related_name]:
                        writer.write_related(rec.pk, item, self.outfiles[related_name])
                else:
                    writer.write_related(rec.pk, rec.data[related_name],
                                         self.outfiles[related_name])

    def make_constants_csv_writer(self):
        """Generate a Record Writer capable of writing out the non-json fields of a Record"""

        def render_date(d):
            return d.astimezone(local_tz).strftime('%Y-%m-%d %H:%M:%S')

        # TODO: Currently this is hard-coded; it may be worthwhile to make this introspect Record
        # to figure out which fields to use, but that will be somewhat involved.
        csv_columns = ['record_id', 'timezone', 'created', 'modified', 'occurred_from',
                       'occurred_to', 'lat', 'lon', 'location_text',
                       'city', 'city_district', 'county', 'neighborhood', 'road',
                       'state', 'weather', 'light']
        # Model field from which to get data for each csv column
        source_fields = {
            'record_id': 'uuid',
            'timezone': None,
            'lat': 'geom',
            'lon': 'geom'
        }

        # Some model fields need to be transformed before they can go into a CSV
        value_transforms = {
            'record_id': lambda uuid: str(uuid),
            'timezone': lambda _: settings.TIME_ZONE,
            'created': render_date,
            'modified': render_date,
            'occurred_from': render_date,
            'occurred_to': render_date,
            'lat': lambda geom: geom.y,
            'lon': lambda geom: geom.x,
        }
        return RecordModelWriter(csv_columns, source_fields, value_transforms)

    def make_related_info_writer(self, info_name, info_definition, include_record_id=True):
        """Generate a RelatedInfoExporter capable of writing out a particular related info field
        :param info_definition: The definitions entry providing the sub-schema to write out.
        """
        # Need to drop Media fields; we can't export them to CSV usefully.
        drop_keys = dict()
        for prop, attributes in info_definition['properties'].items():
            if 'media' in attributes:
                drop_keys[prop] = None
        return RelatedInfoWriter(info_name, info_definition, field_transform=drop_keys,
                                 include_record_id=include_record_id)

    def make_record_and_details_writer(self):
        """Generate a writer to put record fields and details in one CSV"""
        model_writer = self.make_constants_csv_writer()
        details = {key: subschema for key, subschema in (self.schema['definitions'].items())
                   if subschema.get('details') is True}
        details_key = list(details.keys())[0]
        details_writer = self.make_related_info_writer(details_key, details[details_key],
                                                       include_record_id=False)
        return ModelAndDetailsWriter(model_writer, details_writer, details_key)


class ReadOnlyRecordExporter(DriverRecordExporterForPublic):
    """Export only fields which read-only users are allow to access"""

    def __init__(self, schema_obj):
        # Don't write any related info fields, just details only.
        self.schema = schema_obj.schema

        # Make output writers and output files
        self.rec_writer = self.make_record_and_details_writer()
        self.writers = dict()

        self.rec_outfile, self.outfiles = self.setup_output_files()
        self.write_headers()

    def setup_output_files(self):
        """Create the output files necessary for writing CSVs"""
        # Using NamedTemporaryFiles is necessary for creating tarballs containing temp files
        # https://bugs.python.org/issue21044
        rec_outfile = tempfile.NamedTemporaryFile(delete=False, mode="wt")
        outfiles = dict()
        return (rec_outfile, outfiles)


class BaseRecordWriter(object):
    """Base class for some common functions that exporters need"""

    def write_header(self, csv_file):
        """Write the CSV header to csv_file"""
        # Need to sanitize CSV columns to utf-8 before writing
        header_columns = [_utf8(col) for col in self.csv_columns]
        writer = csv.DictWriter(csv_file, fieldnames=header_columns)
        writer.writeheader()


class ModelAndDetailsWriter(BaseRecordWriter):
    """Exports records' model fields, and the *Details field, to a single CSV"""

    def __init__(self, model_writer, details_writer, details_key):
        """Creates a combined writer
        :param model_writer: A RecordModelWriter instance that will be used to write model fields
        :param details_writer: A RelatedInfoWriter instance that will be used to write Details
        """
        self.model_writer = model_writer
        self.details_writer = details_writer
        self.details_key = details_key

    def merge_lines(self, lines_str):
        """Merge lines written by separate CSV writers to a single line by replacing '\r\n' with ','
        """
        return lines_str.replace('\r\n', ',').rstrip(',') + '\r\n'

    def write_header(self, csv_file):
        """Write writer headers to a CSV file"""
        output = io.StringIO()
        self.model_writer.write_header(output)
        self.details_writer.write_header(output)
        csv_file.write(self.merge_lines(output.getvalue()))

    def write_record(self, record, csv_file):
        """Pull data from a record, send to appropriate writers, and then combine output"""
        output = io.StringIO()
        for key in record.data:
            if key == 'driverInterventionDetails':
                if record.data['driverInterventionDetails']['Type']:
                    if record.data['driverInterventionDetails']['Type'].isdigit():
                        try:
                            irap_object = IrapDetail.objects.get(irap_treatment_id=record.data['driverInterventionDetails']['Type'])
                            type_name = irap_object.irap_treatment_name
                            record.data['driverInterventionDetails']['Type'] = record.data['driverInterventionDetails'][
                                                                                   'Type'] + ' - ' + type_name
                        except IrapDetail.DoesNotExist:
                            record.data['driverInterventionDetails']['Type'] = None
        self.model_writer.write_record(record, output)
        self.details_writer.write_related(record.pk, record.data[self.details_key], output)
        csv_file.write(self.merge_lines(output.getvalue()))


class RecordModelWriter(BaseRecordWriter):
    """Exports records' model fields to CSV"""

    def __init__(self, csv_columns, source_fields=dict(), value_transforms=dict()):
        """Creates a record exporter
        :param csv_columns: List of columns names to write out to the CSV.
                            E.g. ['latitude', 'longitude']
        :param source_fields: Dictionary mapping column names to the name of the model field where
                              the appropriate value can be found.
                              E.g. {'latitude': 'geom', 'longitude': 'geom'}
                              Pulls from attributes with the same name as the column name by default
        :param value_transforms: Dictionary mapping column names to functions by which to transform
                                 model field values before writing to the CSV.
                                 E.g. {'latitude': lambda geom: geom.y}
                                 If a field is not included here, it will be used directly
        """
        self.csv_columns = csv_columns
        self.source_fields = source_fields
        self.value_transforms = value_transforms

    def write_record(self, record, csv_file):
        """Pull field data from record object, transform, write to csv_file"""
        output_data = dict()
        for column in self.csv_columns:
            model_value = self.get_model_value_for_column(record, column)
            csv_val = self.transform_model_value(model_value, column)
            # output_data[column] = _utf8(csv_val)
            output_data[column] = csv_val
        writer = csv.DictWriter(csv_file, fieldnames=self.csv_columns)
        writer.writerow(output_data)

    def get_model_value_for_column(self, record, column):
        """Gets the value from the appropriate model field to populate column"""
        # Get the value from record.<source_field> if a <source_field> is defined for <column>,
        # otherwise get it from record.<column>
        model_field = self.source_fields.get(column, column)
        if model_field is None:
            return None
        return getattr(record, model_field)

    def transform_model_value(self, value, column):
        """Transforms value into an appropriate value for column"""
        # Pass the value through any necessary transformation before output.
        val_transform = self.value_transforms.get(column, lambda v: v)
        return val_transform(value)


class RelatedInfoWriter(BaseRecordWriter):
    """Exports related info properties to CSV"""

    def __init__(self, info_name, info_definition, field_transform=dict(), include_record_id=True):
        # Construct a field name mapping; this allows dropping Media fields from CSVs and
        # allows renaming _localid to something more useful. The final output will be a mapping
        # of all fields in the related info definition to the corresponding field that should
        # be output in the CSV. If a field name is mapped to None then it is dropped.
        self.property_transform = field_transform
        try:
            for prop in info_definition['properties']:
                if prop not in self.property_transform:
                    self.property_transform[prop] = prop
        except KeyError:
            raise ValueError("Related info definition has no 'properties'; can't detect fields")
        self.property_transform['_localId'] = info_name + '_id'
        info_columns = [col for col in self.property_transform.values() if col is not None]
        self.output_record_id = include_record_id
        if self.output_record_id:
            # Need to label every row with the id of the record it relates to
            self.csv_columns = ['record_id'] + info_columns
        else:
            self.csv_columns = info_columns
        self.is_multiple = info_definition.get('multiple', False)

    def write_related(self, record_id, related_info, csv_file):
        """Transform related_info and write to csv_file"""
        # Transform
        output_data = self.transform_value_keys(related_info)

        # Append record_id
        if self.output_record_id:
            output_data['record_id'] = record_id

        # Write
        writer = csv.DictWriter(csv_file, fieldnames=self.csv_columns)

        if "Location Approximate" in output_data:
            loc_appro = output_data["Location Approximate"]
            if type(loc_appro) == bytes:
                decode_loc_appro = loc_appro.decode('utf-8')
                if type(decode_loc_appro) == str:
                    list_of_loc_appro = eval(decode_loc_appro)
                    try:
                        output_data["Location Approximate"] = list_of_loc_appro[0]
                    except:
                        output_data["Location Approximate"] = None

        if "Email of Encoder" in output_data:
            email_encoder = output_data["Email of Encoder"]
            if type(email_encoder) == bytes:
                decode_email = email_encoder.decode('utf-8')
                try:
                    list_of_email = eval(decode_email)
                    output_data["Email of Encoder"] = list_of_email[0]
                except:
                    pass

        if "Severity" in output_data:
            mynewstr = ''
            severityval = output_data["Severity"]
            if type(severityval) == bytes:
                listof_serverity = severityval.decode('utf-8')
                if len(eval(listof_serverity)) == 1:
                    mynewstr = eval(listof_serverity)[0]
                else:
                    for i in eval(listof_serverity):
                        if len(listof_serverity) - 1 == listof_serverity.index(i):
                            mynewstr = mynewstr + i
                        else:
                            mynewstr = mynewstr + i + "|"

                if mynewstr.endswith("|"):
                    mynewstr = mynewstr[:-1]
                output_data["Severity"] = mynewstr.strip()

        writer.writerow(output_data)

    def transform_value_keys(self, related_info):
        """Set incoming values to new keys in output_data based on self.property_transform"""
        output_data = dict()
        for in_key, out_key in self.property_transform.items():
            if out_key is not None:
                try:
                    # Assign the value of the input data to the renamed key in the output data
                    output_data[out_key] = _utf8(related_info.pop(in_key))
                except KeyError:
                    # in_key doesn't exist in input; this is fine, the CSV writer will handle it
                    pass
        return output_data
