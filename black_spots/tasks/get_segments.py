import fiona
# from fiona.crs import from_epsg
from fiona.crs import CRS
from functools import partial
import csv
import gc
import glob
import itertools
import logging
import os
import pyproj
import pytz
import rtree
import shutil
from math import ceil
import tarfile
import tempfile
import billiard as multiprocessing
from shapely.geometry import (
    mapping, shape, LineString, MultiPoint, Point, Polygon, MultiLineString, GeometryCollection
)

from shapely.ops import transform, unary_union
from DRIVER import settings

from django.core.files import File
from django.db import transaction

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

from black_spots.models import BlackSpotRecordsFile, RoadSegmentsShapefile

from celery import shared_task
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

INTERSECTION_BUFFER_UNITS = int(os.getenv('INTERSECTION_BUFFER_UNITS', '5'))
MAX_LINE_UNITS = int(os.getenv('MAX_LINE_UNITS', '200'))
RECORD_PROJECTION = os.getenv('RECORD_PROJECTION', 'epsg:4326')
TIMEZONE = pytz.timezone(settings.TIME_ZONE)
MATCH_TOLERANCE = int(os.getenv('MATCH_TOLERANCE', '5'))
RECORD_COL_ID = os.getenv('RECORD_COL_ID', 'record_id')
RECORD_COL_X = os.getenv('RECORD_COL_X', 'lon')
RECORD_COL_Y = os.getenv('RECORD_COL_Y', 'lat')
RECORD_COL_OCCURRED = os.getenv('RECORD_COL_OCCURRED', 'occurred_from')
RECORD_COL_SEVERE = os.getenv('RECORD_COL_SEVERE', 'Severity')
RECORD_COL_SEVERE_VALS = os.getenv('RECORD_COL_SEVERE_VALS', 'Fatal,Injury')
RECORD_COL_PRECIP = os.getenv('RECORD_COL_PRECIP', 'weather')
RECORD_COL_PRECIP_VALS = os.getenv('RECORD_COL_PRECIP_VALS',
                                   'rain,hail,sleet,snow,thunderstorm,tornado')
COMBINED_SEGMENTS_SHP_NAME = os.getenv('COMBINED_SEGMENTS_SHP_NAME', 'combined_segments.shp')
TILE_MAX_UNITS = int(os.getenv('TILE_MAX_UNITS', '3000'))


@shared_task
def cleanup(shp_dir, tar_dir):
    """Cleanup shapefile and tarfile; call as errback"""
    try:
        shutil.rmtree(shp_dir, True)
    except UnboundLocalError:
        pass
    try:
        shutil.rmtree(tar_dir, True)
    except UnboundLocalError:
        pass


@shared_task
@transaction.atomic
def create_segments_tar(combined_segments_shp_dir):
    """Create a RoadSegmentsShapefile out of a shpfile"""
    logger.info('Creating tar file from shapefile segments')
    tar_output_dir = tempfile.mkdtemp()
    tarball_path = create_tarball(combined_segments_shp_dir, tar_output_dir)

    road_segments_shpfile = RoadSegmentsShapefile.objects.create()
    logger.info('Created database record: %s', road_segments_shpfile.uuid)

    with open(tarball_path, 'rb') as f:
        tarball_file = File(f)
        save_name = '{}.tgz'.format(road_segments_shpfile.uuid)
        logger.info('Saving tarball with django-storages: %s', tarball_path)
        road_segments_shpfile.shp_tgz.save(save_name, tarball_file)
    return str(road_segments_shpfile.pk)


@shared_task
def get_segments_shp(path_to_roads_shp, records_csv_uuid, road_srid):
    """Save segments to a shapefile and save in model

    Args:
    :param path_to_roads_shp: (str) path to shapefile
    :param records_csv_uuid: (str) UUID for the BlackSpotRecordsFile
    :param road_srid: (int) EPSG ID of roads shapefile projection
    """
    try:
        logger.info('Reading Records CSV with uuid: %s', records_csv_uuid)
        record_buffers = get_record_buffers(road_srid, records_csv_uuid)
        logger.info('Num record buffers: %s', len(record_buffers))

        logger.info('Reading Roads Shapefile: %s', path_to_roads_shp)
        roads, road_bounds = read_roads(path_to_roads_shp, record_buffers)
        logger.info('Number of roads after filtering: %s', len(roads))

        logger.info('Obtaining intersection buffers')
        int_buffers = get_intersection_buffers(roads, road_bounds, INTERSECTION_BUFFER_UNITS,
                                               TILE_MAX_UNITS)
        logger.info('Obtaining intersections')
        combined_segments = get_intersection_parts(roads, int_buffers, MAX_LINE_UNITS)
        logger.info('Deleting buffers; no longer used')
        del int_buffers  # Free mem, this is a big 'un.
        gc.collect()

        logger.info('Writing shapefile segments')
        shp_output_dir = tempfile.mkdtemp()

        segments_shp_path = os.path.join(shp_output_dir, COMBINED_SEGMENTS_SHP_NAME)
        write_segments_shp(segments_shp_path, road_srid, combined_segments)

        logger.info('Cleaning up old lines shapefile')
        for f in glob.glob(path_to_roads_shp.replace('.shp', '.*')):
            logger.info('Removing file: {}'.format(f))
            os.remove(f)
    except:
        logger.exception('Error generating segments shapefile')
        raise
    return shp_output_dir


from pyproj import Transformer


def get_record_buffers(road_srid, records_csv_uuid):
    """Returns a list of all records, buffered by the configured MATCH_TOLERANCE
    :param road_srid: EPSG ID of roads shapefile projection
    :param records_csv_uuid: UUID of the BlackSpotRecordsFile CSV
    """
    road_projection = {'init': 'epsg:{}'.format(road_srid)}
    record_projection = {'init': RECORD_PROJECTION}
    # project = partial(
    #     pyproj.transform,
    #     pyproj.Proj(record_projection),
    #     pyproj.Proj(road_projection)
    # )
    transformer = Transformer.from_crs(record_projection, road_projection, always_xy=True)
    project = lambda x, y: transformer.transform(x, y)
    record_buffers = []
    with BlackSpotRecordsFile.objects.get(uuid=records_csv_uuid).csv as records_csv:
        records_csv.open('rb')
        try:
            reader = csv.DictReader((line.decode('utf-8').strip() for line in records_csv))
            for row in reader:
                try:
                    longitude = float(row[RECORD_COL_X].strip().replace("b'", "").replace("'", ""))  # Clean the value
                    latitude = float(row[RECORD_COL_Y].strip().replace("b'", "").replace("'", ""))
                    record_point = transform(project, Point(longitude,
                                                            latitude))
                    record_buffers.append(record_point.buffer(MATCH_TOLERANCE))
                except RuntimeError:
                    continue
        finally:
            records_csv.close()

    return record_buffers


def create_tarball(shp_dir, tar_dir):
    """Creates gzipped tarball of shapefile

    :params shp_dir: output directory of shapefile
    :params tar_dir: output directory for gzipped tarball
    """
    filename = 'road_segments.tar.gz'
    tar_path = os.path.join(tar_dir, filename)
    with tarfile.open(tar_path, 'w:gz') as tarball:
        tarball.add(shp_dir, arcname='segments')

    return tar_path



def get_intersections(roads):
    """Calculates the intersection points of all roads
    :param roads: List of shapely geometries representing road segments
    """
    logger.info('getting intersections')
    intersections = []

    for road1, road2 in itertools.combinations(roads, 2):
        if road1.intersects(road2):
            intersection = road1.intersection(road2)
            if intersection.geom_type == 'Point':
                intersections.append(intersection)
            elif intersection.geom_type == 'MultiPoint':
                intersections.extend(intersection.geoms)
            elif intersection.geom_type == 'MultiLineString':
                multiLine = list(intersection.geoms)
                first_coords = multiLine[0].coords[0]
                last_coords = multiLine[-1].coords[-1]
                intersections.append(Point(first_coords))
                intersections.append(Point(last_coords))
            elif intersection.geom_type == 'GeometryCollection':
                for geom in intersection.geoms:
                    sub_intersections = get_intersections([geom])  # Fix recursive call
                    if sub_intersections.geom_type == "Point":
                        intersections.append(sub_intersections)
                    elif sub_intersections.geom_type == "MultiPoint":
                        intersections.extend(sub_intersections.geoms)

    # The unary_union removes duplicate points
    unioned = unary_union(intersections)

    # Ensure the result is a MultiPoint, since calling functions expect an iterable
    if unioned.geom_type == 'Point':
        unioned = MultiPoint([unioned])

    return unioned


def should_keep_road(road, road_shp, record_buffers_index):
    """Returns true if road should be considered for segmentation
    :param road: Dictionary representation of the road (with properties)
    :param roads_shp: Shapely representation of the road
    :param record_buffers_index: RTree index of the record_buffers
    """
    # If the road has no nearby records, then we can discard it early on.
    # This provides a major optimization since the majority of roads don't have recorded accidents.
    try:
        xu=record_buffers_index.intersection(road_shp.bounds)
    except:
        xu=[]
    if not len(list(xu)):
        return False

    props = road['properties']
    if props.get('highway', None) not in ['path', 'footway', None]:
        return True

    # We're only interested in non-bridge, non-tunnel highways
    # 'class' is optional, so only consider it when it's available.
    if ('class' not in props or props['class'] == 'highway'
            and props['bridge'] == 0
            and props['tunnel'] == 0):
        return True
    return False


def read_roads(roads_shp, record_buffers):
    """Reads shapefile and extracts roads and projection
    :param roads_shp: Path to the shapefile containing roads
    :param record_buffers: List of shapely geometries representing buffered event points
    """
    # Create a spatial index for record buffers to efficiently find intersecting roads
    record_buffers_index = rtree.index.Index()
    for idx, record_buffer in enumerate(record_buffers):
        record_buffers_index.insert(idx, record_buffer.bounds)

    # shp_file = fiona.open(roads_shp)
    shp_file = list(fiona.open(roads_shp))  # Convert to list to avoid iterator exhaustion
    roads = []
    road_bounds = [shape(road['geometry']).bounds for road in shp_file]  # Get bounding box

    logger.info('Number of roads in shapefile: {}'.format(len(shp_file)))
    for idx, road in enumerate(shp_file):
        road_shp = shape(road['geometry'])
        if should_keep_road(road, road_shp, record_buffers_index):
            roads.append(road_shp)
        logger.info('Remaining: {}'.format(len(shp_file) - idx - 1))

    return (roads, road_bounds)  # ✅ Return correct bounding box


def get_intersection_buffers(roads, road_bounds, intersection_buffer_units, tile_max_units):
    """Buffers all intersections
    :param roads: List of shapely geometries representing road segments
    :param road_bounds: Bounding box of the roads shapefile
    :param intersection_buffer_units: Number of units to use for buffer radius
    :param tile_max_units: Maxium number of units for each side of a tile
    """
    # As an optimization, the road network is divided up into a grid of tiles,
    # and intersections are calculated within each tile.
    def roads_per_tile_iter():
        """Generator which yields a set of roads for each tile"""
        min_x, min_y, max_x, max_y = road_bounds
        bounds_width = max_x - min_x
        bounds_height = max_y - min_y
        x_divisions = ceil(bounds_width / tile_max_units)
        y_divisions = ceil(bounds_height / tile_max_units)
        tile_width = bounds_width / x_divisions
        tile_height = bounds_height / y_divisions

        # Create a spatial index for roads to efficiently match up roads to tiles
        logger.info('Generating spatial index for intersections')
        roads_index = rtree.index.Index()
        for idx, road in enumerate(roads):
            roads_index.insert(idx, road.bounds)

        logger.info('Number of tiles: {}'.format(int(x_divisions * y_divisions)))
        for x_offset in range(0, int(x_divisions)):
            for y_offset in range(0, int(y_divisions)):
                road_ids_in_tile = roads_index.intersection([
                    min_x + x_offset * tile_width,
                    min_y + y_offset * tile_height,
                    min_x + (1 + x_offset) * tile_width,
                    min_y + (1 + y_offset) * tile_height
                ])
                roads_in_tile = [roads[road_id] for road_id in road_ids_in_tile]
                logger.info('Roads in tile: {}'.format(int(len(roads_in_tile))))

                if len(roads_in_tile) > 1:
                    yield roads_in_tile

    # Allocate one worker per core, and parallelize the discovery of intersections
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    tile_intersections = list(pool.imap(get_intersections, roads_per_tile_iter()))
    pool.close()
    pool.join()

    logger.info('Buffering intersections')
    # Note: tile_intersections is a list of multipoints (which is a list of points).
    # itertools.chain.from_iterable flattens the list into a list of single points.
    buffered_intersections = [
        intersection.buffer(intersection_buffer_units)
        for tile in tile_intersections
        for intersection in (tile.geoms if tile.geom_type == "MultiPoint" else [tile])  # ✅ Unpack MultiPoint
    ]

    # If intersection buffers overlap, union them to treat them as one
    logger.info('Performing unary union on buffered intersections')
    return unary_union(buffered_intersections)


def split_line(line, max_line_units):
    """Checks the line's length and splits in half if larger than the configured max
    :param line: Shapely line to be split
    :param max_line_units: The maximum allowed length of the line
    """
    if line.length <= max_line_units:
        return [line]

    half_length = line.length / 2
    coords = list(line.coords)
    for idx, point in enumerate(coords):
        proj_dist = line.project(Point(point))
        if proj_dist == half_length:
            return [LineString(coords[:idx + 1]), LineString(coords[idx:])]

        if proj_dist > half_length:
            mid_point = line.interpolate(half_length)
            head_line = LineString(coords[:idx] + [(mid_point.x, mid_point.y)])
            tail_line = LineString([(mid_point.x, mid_point.y)] + coords[idx:])
            return split_line(head_line, max_line_units) + split_line(tail_line, max_line_units)


from shapely.geometry import Polygon
def get_intersection_parts(roads, int_buffers, max_line_units):
    """Finds all segments that intersect the buffers, and all that don't
    :param roads: List of shapely geometries representing road segments
    :param int_buffers: List of shapely polygons representing intersection buffers
    :param max_line_units: The maximum allowed length of the line
    """


    # Create a spatial index for intersection buffers to efficiently find intersecting segments
    int_buffers_index = rtree.index.Index()

    # Ensure int_buffers is a list of Polygons
    if isinstance(int_buffers, GeometryCollection):
        int_buffers = [geom for geom in int_buffers.geoms if isinstance(geom, Polygon)]  # Extract only polygons
    elif not isinstance(int_buffers, list):
        int_buffers = [int_buffers]  # Convert single object to a list

    for idx, intersection_buffer in enumerate(int_buffers):
        if isinstance(intersection_buffer, Polygon):
            bounds = intersection_buffer.bounds  # Tuple (minx, miny, maxx, maxy)
            int_buffers_index.insert(idx, bounds)
        else:
            logger.info(f"Warning: Skipping non-Polygon object at index {idx}: {type(intersection_buffer)}")

    segments_map = {}
    non_int_lines = []
    for road in roads:
        road_int_buffers = []
        for idx in int_buffers_index.intersection(road.bounds):
            int_buffer = int_buffers[idx]
            if int_buffer.intersects(road):
                if idx not in segments_map:
                    segments_map[idx] = []
                segments_map[idx].append(int_buffer.intersection(road))
                road_int_buffers.append(int_buffer)

        # Collect the non-intersecting segments
        if len(road_int_buffers) > 0:
            diff = road.difference(unary_union(road_int_buffers))
            if 'LineString' == diff.type:
                non_int_lines.append(diff)
            elif 'MultiLineString' == diff.type:
                non_int_lines.extend([line for line in diff])
        else:
            non_int_lines.append(road)

    # Union all lines found within a buffer, treating them as a single unit
    int_multilines = [unary_union(lines) for _, lines in segments_map.items()]

    # Split any long non-intersecting segments. It's not important that they
    # be equal lengths, just that none of them are exceptionally long.
    split_non_int_lines = []
    for line in non_int_lines:
        split_non_int_lines.extend(split_line(line, max_line_units))

    # Return a tuple of intersection multilines and non-intersecting segments
    return int_multilines + split_non_int_lines


# import fiona
# from fiona.crs import CRS
# from shapely.geometry import mapping, LineString, MultiLineString


def write_segments_shp(segments_shp_path, road_srid, segments):
    """Writes all segments to shapefile (both intersections and individual segments)

    :param segments_shp_path: Path to shapefile to write
    :param road_srid: EPSG ID of projection of road data
    :param segments: List of Shapely LineString or MultiLineString geometries
    """

    schema = {
        'geometry': 'MultiLineString',  # Ensuring schema expects MultiLineString
        'properties': {
            'id': 'int',
            'inter': 'int',
            'length': 'float',
            'lines': 'int',
            'pointx': 'float',
            'pointy': 'float'
        }
    }

    logger.info("Opening shapefile for output")
    with fiona.open(segments_shp_path, 'w', driver='ESRI Shapefile',
                    schema=schema, crs=CRS.from_epsg(road_srid)) as output:
        logger.info("Beginning to write segments")
        for idx, segment in enumerate(segments):
            # ✅ Convert LineString to MultiLineString if needed
            if isinstance(segment, LineString):
                segment = MultiLineString([segment])  # Convert to MultiLineString

            is_intersection = isinstance(segment, MultiLineString)
            data = {
                'id': idx,
                'inter': 1 if is_intersection else 0,
                'length': segment.length,
                'lines': len(segment.geoms) if is_intersection else 1,
                'pointx': segment.centroid.x,
                'pointy': segment.centroid.y,
            }

            output.write({
                'geometry': mapping(segment),
                'properties': data
            })

            if idx % 1000 == 0:
                logger.debug("Wrote {} segments".format(idx))

    logger.info("Shapefile writing complete.")
