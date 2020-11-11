"""
This is a chunk which includes all steps
to use MAJA installed on peps server
to preprocess sentinel-2 products.
This script is took from and given full credit to
https://github.com/olivierhagolle/maja_peps and
https://github.com/olivierhagolle/peps_download
Maintainer: Lei Song (lsong@clarku.edu)
"""
import json
import logging
import optparse
import os
import os.path
import re
import shutil
import sys
import time
import zipfile
from datetime import date, datetime
from os.path import exists, join
from .internal_functions import _divide_chunks
import geojson
import requests
import yaml


class OptionParser(optparse.OptionParser):
    """The class for OptionParser.
    Add a function check_required
    """

    def check_required(self, opt):
        option = self.get_option(opt)

        # Assumes the option's 'default' is set to None!
        if getattr(self.values, option.dest) is None:
            self.error("%s option not supplied" % option)


class GeoJSON:
    """GeoJSON class which allows to calculate bbox
    Attributes:
        coords (list): the list of coordinates.
        features_count (int): the number of features.
    """

    def __init__(self, gj_object):
        if gj_object['type'] == 'FeatureCollection':
            self.coords = list(self._flatten([f['geometry']['coordinates']
                                              for f in gj_object['features']]))
            self.features_count = len(gj_object['features'])
        elif gj_object['type'] == 'Feature':
            self.coords = list(self._flatten([
                gj_object['geometry']['coordinates']]))
            self.features_count = 1
        else:
            self.coords = list(self._flatten([gj_object['coordinates']]))
            self.features_count = 1

    def _flatten(self, l):
        for val in l:
            if isinstance(val, list):
                for subval in self._flatten(val):
                    yield subval
            else:
                yield val

    def bbox(self):
        return [min(self.coords[::2]), min(self.coords[1::2]),
                max(self.coords[::2]), max(self.coords[1::2])]


class ParserConfig:
    """The config object
    Attributes:
            tile (str): the list of coordinates.
            geojson (int): the number of features.
            location (str): town name (pick one which is not too frequent to avoid confusions).
            lat (float): latitude in decimal degrees.
            lon (float): longitude in decimal degrees.
            latmin (float): min latitude in decimal degrees.
            latmax (float): max latitude in decimal degrees.
            lonmin (float): min longitude in decimal degrees.
            lonmax (float): max longitude in decimal degrees.
            write_dir (str): Path where the products should be downloaded.
            collection (str): Collection within theia collections, ['S1', 'S2', 'S2ST', 'S3'].
            product_type (str): GRD, SLC, OCN (for S1) | S2MSI1C S2MSI2Ap (for S2).
            sensor_mode (str): EW, IW , SM, WV (for S1) | INS-NOBS, INS-RAW (for S2).
            no_download (bool): Do not download products, just print curl command.
            start_date (str): start date, fmt('2015-12-22').
            end_date (str): end date, fmt('2015-12-22').
            clouds (int): Maximum cloud coverage.
            windows (bool): For windows usage, True if use, otherwise False.
            extract (bool): Extract and remove zip file after download.
            search_json_file (str): Output search JSON filename.
            sat (str): satellite, S1A, S1B, S2A, S2B, S3A, S3B.
            orbit (int): Orbit Path number.
            maja_log (str): path to store log file of maja.
            log_dir( str): path to store script logs.
    """

    def __init__(self, config_path):
        config = parse_config(config_path)
        self.auth = config_path
        self.maja_log = join(config['dirs']['dst_dir'],
                             config['dirs']['maja_log'])

        # Set download path
        self.dst_dir = config['dirs']['dst_dir']
        self.log_dir = config['dirs']['log_dir']
        if config['dirs']['download_path'] is None:
            self.write_dir = config['dirs']['dst_dir']
        else:
            self.write_dir = join(config['dirs']['dst_dir'],
                                  config['dirs']['download_path'])

        if config['dirs']['processed_path'] is None:
            self.processed_dir = config['dirs']['dst_dir']
        else:
            self.processed_dir = join(config['dirs']['dst_dir'],
                                    config['dirs']['processed_path'])

        # Set geometry with the order tile, geojson, bbox, and point
        config = config['sentinel']
        self.tile = None
        self.geojson = None
        self.location = None
        self.lat = None
        self.lon = None
        self.latmin = None
        self.latmax = None
        self.lonmin = None
        self.lonmax = None
        if config['tile'] is not None:
            print("Use tile for query.")
            self.tile = config['tile']
        elif config['geojson'] is not None:
            print("Use geojson for query.")
            print("Warning: if the single feature is too big, only 500 imagery will be back.")
            print("Suggestion: use a small geojson each time.")
            self.geojson = config['geojson']
        elif config['bbox'] is not None:
            print("Use bbox to query.")
            self.latmin = config['bbox'][0]
            self.latmax = config['bbox'][1]
            self.lonmin = config['bbox'][2]
            self.lonmax = config['bbox'][3]
        elif config['point'] is not None:
            print("Use coordinate to query.")
            self.lon = config['point'][0]
            self.lat = config['point'][1]
        elif config['location'] is not None:
            print("Use location to query.")
            self.location = config['location']

        # Set sentinel parameter
        self.collection = config['platformname']
        self.product_type = config['producttype']
        self.sensor_mode = config['sensoroperationalmode']
        self.no_download = not config['download']
        self.start_date = config['date_start']
        self.end_date = config['date_end']
        if config['clouds'] is None:
            self.clouds = 100
        else:
            self.clouds = int(config['clouds'])
        self.windows = config['windows']
        self.extract = config['extract']
        self.search_json_file = config['catalog_json']
        self.sat = config['satellite']
        self.orbit = config['orbit']


def parse_config(auth_file_path):
    """The script to parse config yaml.

    Args:
        auth_file_path (str): the path of yaml file.
    """
    with open(auth_file_path, 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)
        return config


def _query_catalog(options, query_geom, start_date, end_date, logger):
    """The script to query catalog from peps.

    Args:
        options (ParserConfig): the config object.
        query_geom (list or str): the geom for query.
        start_date (str): the start date to query.
        end_date (str): the end date to query.
        logger (logging.Logger): the logger object to store logs.
    """
    # Parse catalog
    # If the query geom is a geojson with more than 1 feature
    if isinstance(query_geom, list):
        logger.info('Query based on geojson with multiple features.')
        json_file_tmp = 'tmp.json'
        json_all = {"type": "FeatureCollection",
                    "properties": {},
                    "features": []}
        for i in range(0, len(query_geom)):
            each = query_geom[i]
            latmin = each[1]
            latmax = each[3]
            lonmin = each[0]
            lonmax = each[2]
            query_geom_each = 'box={lonmin},{latmin},{lonmax},{latmax}' \
                .format(latmin=latmin, latmax=latmax,
                        lonmin=lonmin, lonmax=lonmax)
            if (options.product_type is None) and (options.sensor_mode is None):
                search_catalog = "curl -k -o {} https://peps.cnes.fr/resto/api/" \
                                 "collections/{}/search.json?{}\&startDate={}" \
                                 "\&completionDate={}\&maxRecords=500" \
                    .format(json_file_tmp, options.collection,
                            query_geom_each, start_date, end_date)
            else:
                product_type = "" if options.product_type is None else options.product_type
                sensor_mode = "" if options.sensor_mode is None else options.sensor_mode
                search_catalog = 'curl -k -o {} https://peps.cnes.fr/resto/api/' \
                                 'collections/{}/search.json?{}\&startDate={}' \
                                 '\&completionDate={}\&maxRecords=500' \
                                 '\&productType={}\&sensorMode={}' \
                    .format(json_file_tmp, options.collection,
                            query_geom_each, start_date, end_date,
                            product_type, sensor_mode)
            if options.windows:
                search_catalog = search_catalog.replace('\&', '^&')
            os.system(search_catalog)
            time.sleep(5)

            try:
                with open(json_file_tmp) as data_file:
                    json_each = json.load(data_file)
                    if 'ErrorCode' in json_each:
                        logger.error("Error in query of {}th feature: {}"
                                     .format(i, json_each['ErrorMessage']))
                    else:
                        for n in range(0, len(json_each['features'])):
                            json_each['features'][n]['properties']['no_geom'] = i
                        json_all['features'].extend(json_each['features'])
                os.remove(json_file_tmp)
            except OSError:
                logger.warn("Failed to search for the {}th tile.".format(i))

        # Write json_all as search_json_file
        with open(options.search_json_file, 'w') as f:
            json.dump(json_all, f)
        logger.info("Write gathered search json to {}.".format(options.search_json_file))

    # Regular condition
    else:
        logger.info("Query based on regular conditions.")
        if (options.product_type is None) and (options.sensor_mode is None):
            search_catalog = "curl -k -o {} https://peps.cnes.fr/resto/api/" \
                             "collections/{}/search.json?{}\&startDate={}" \
                             "\&completionDate={}\&maxRecords=500" \
                .format(options.search_json_file, options.collection,
                        query_geom, start_date, end_date)
        else:
            product_type = "" if options.product_type is None else options.product_type
            sensor_mode = "" if options.sensor_mode is None else options.sensor_mode
            search_catalog = 'curl -k -o {} https://peps.cnes.fr/resto/api/' \
                             'collections/{}/search.json?{}\&startDate={}' \
                             '\&completionDate={}\&maxRecords=500' \
                             '\&productType={}\&sensorMode={}' \
                .format(options.search_json_file, options.collection,
                        query_geom, start_date, end_date,
                        product_type, sensor_mode)

        if options.windows:
            search_catalog = search_catalog.replace('\&', '^&')

        logger.info(search_catalog)
        os.system(search_catalog)
        time.sleep(5)


def check_rename(tmpfile, options, prod, prodsize, logger):
    """The script to check downloaded file and rename it.

    Args:
        tmpfile (str); the temp file name.
        options (ParserConfig): the config object.
        prod (str): the name of imagery.
        prodsize (int): the correct size of full imagery.
        logger (logging.Logger): the logger object to store logs.
    """
    logger.info("{} {}".format(os.path.getsize(tmpfile), prodsize))
    if os.path.getsize(tmpfile) != prodsize:
        with open(tmpfile) as f_tmp:
            try:
                tmp_data = json.load(f_tmp)
                logger.warning("Result is a text file (might come from a wrong password file)")
                logger.info(tmp_data)
                sys.exit(-1)
            except ValueError:
                logger.warning("\nDownload was not complete, tmp file removed")
                os.remove(tmpfile)
                return

    zfile = "{}/{}.zip".format(options.write_dir, prod)
    os.rename(tmpfile, zfile)

    # Unzip file
    if options.extract and os.path.exists(zfile):
        try:
            with zipfile.ZipFile(zfile, 'r') as zf:
                safename = zf.namelist()[0].replace('/', '')
                zf.extractall(options.write_dir)
            safedir = os.path.join(options.write_dir, safename)
            if not os.path.isdir(safedir):
                raise Exception('Unzipped directory not found: ', zfile)

        except Exception as e:
            logger.warning(e)
            logger.warning('Could not unzip file: ' + zfile)
            os.remove(zfile)
            logger.warning('Zip file removed.')
            return
        else:
            logger.info('Product saved as : ' + safedir)
            os.remove(zfile)
            return
    logger.info("Product saved as : " + zfile)


def check_params(begin_date, stop_date, tileid, orbit=None):
    """Check the parameters

    Args:
        begin_date (str): Starting Date, format : str(XXXX-XX-XX)
        stop_date (str): End date, format : str(XXXX-XX-XX)
        tileid (str): MGRS tile ID
        orbit (int): relative orbit number
    """
    # Check dates
    begin_date = begin_date.split("-")
    stop_date = stop_date.split("-")
    if len(begin_date[0]) != 4 or len(begin_date[1]) != 2 or \
            len(begin_date[2]) != 2 or len(stop_date[0]) != 4 or \
            len(stop_date[1]) != 2 or len(stop_date[2]) != 2:
        raise ValueError("The date format is incorrect")

    begin_date = datetime(int(begin_date[0]), int(begin_date[1]), int(begin_date[2]))
    stop_date = datetime(int(stop_date[0]), int(stop_date[1]), int(stop_date[2]))

    days = (stop_date - begin_date).days

    if days < 55 or days > 366:
        raise ValueError("The time interval must be between 2 months and 1 year")

    # Check orbit number
    if orbit:
        if orbit > 143 or orbit < 1:
            raise ValueError("The relative orbit number must be between 1 and 143")

    # Check tile regex
    re_tile = re.compile("^[0-6][0-9][A-Za-z]([A-Za-z]){0,2}%?$")
    if not re_tile.match(tileid):
        raise ValueError("The tile ID is in the wrong format")


def downloadFile(url, file_name, email, password):
    """Download file

    Args:
        url (str): url to download.
        file_name (str): the file name.
        email (str): peps email.
        password (str): peps password.
    """
    r = requests.get(url, auth=(email, password), stream=True, verify=False)
    with open(file_name, 'wb') as f:
        shutil.copyfileobj(r.raw, f)


def getURL(url, file_name, email, password, logger):
    """Get URL

    Args:
        url (str): url to download.
        file_name (str): the file name.
        email (str): peps email.
        password (str): peps password.
        logger (logging.Logger): logger object to store logs.
    """
    req = requests.get(url, auth=(email, password), verify=False)
    with open(file_name, "w") as f:
        if sys.version_info[0] < 3:
            f.write(req.text.encode('utf-8'))
        else:
            f.write(req.text)
        if req.status_code == 200:
            logger.info("Request OK.")
        else:
            logger.error("Wrong request status {}".format(str(req.status_code)))
            sys.exit(-1)


def parse_catalog(options, logger):
    """The script to parse search json.

    Args:
        options (ParserConfig): the config object.
        logger (logging.Logger): the logger object to store logs.
    """
    # Filter catalog result
    with open(options.search_json_file) as data_file:
        data = json.load(data_file)

    if 'ErrorCode' in data:
        logger.error(data['ErrorMessage'])
        sys.exit(-2)

    # Get unique features
    # Remove no_geom item
    try:
        for i in range(0, len(data["features"])):
            del data['features'][i]['properties']['no_geom']
    except:
        pass
    # Remove duplicates
    result = []
    for i in range(0, len(data['features'])):
        each = data['features'][i]
        if each not in result:
            result.append(each)
    data['features'] = result

    # Sort data
    download_dict = {}
    storage_dict = {}
    size_dict = {}
    if len(data["features"]) > 0:
        for i in range(len(data["features"])):
            prod = data["features"][i]["properties"]["productIdentifier"]
            feature_id = data["features"][i]["id"]
            try:
                storage = data["features"][i]["properties"]["storage"]["mode"]
                platform = data["features"][i]["properties"]["platform"]
                resourceSize = int(data["features"][i]["properties"]["resourceSize"])
                if storage == "unknown":
                    logger.error('Found a product with "unknown" status : %s' % prod)
                    logger.error("Product %s cannot be downloaded" % prod)
                    logger.error('Please send and email with product name to peps admin team : exppeps@cnes.fr')
                else:
                    # parse the orbit number
                    orbitN = data["features"][i]["properties"]["orbitNumber"]
                    if platform == 'S1A':
                        # calculate relative orbit for Sentinel 1A
                        relativeOrbit = ((orbitN - 73) % 175) + 1
                    elif platform == 'S1B':
                        # calculate relative orbit for Sentinel 1B
                        relativeOrbit = ((orbitN - 27) % 175) + 1

                    if options.orbit is not None:
                        if platform.startswith('S2'):
                            if prod.find("_R%03d" % options.orbit) > 0:
                                download_dict[prod] = feature_id
                                storage_dict[prod] = storage
                                size_dict[prod] = resourceSize
                        elif platform.startswith('S1'):
                            if relativeOrbit == options.orbit:
                                download_dict[prod] = feature_id
                                storage_dict[prod] = storage
                                size_dict[prod] = resourceSize
                    else:
                        download_dict[prod] = feature_id
                        storage_dict[prod] = storage
                        size_dict[prod] = resourceSize
            except:
                pass

        # cloud cover criteria:
        if options.collection[0:2] == 'S2':
            logger.info("Check cloud cover criteria.")
            for i in range(len(data["features"])):
                prod = data["features"][i]["properties"]["productIdentifier"]
                if data["features"][i]["properties"]["cloudCover"] > options.clouds:
                    del download_dict[prod], storage_dict[prod], size_dict[prod]

        # Selection of specific satellite
        if options.sat is not None:
            for i in range(len(data["features"])):
                prod = data["features"][i]["properties"]["productIdentifier"]
                if data["features"][i]["properties"]["platform"] != options.sat:
                    try:
                        del download_dict[prod], storage_dict[prod], size_dict[prod]
                    except KeyError:
                        pass

        for prod in download_dict.keys():
            logger.info("{} {}".format(prod, storage_dict[prod]))
    else:
        logger.warning("No product corresponds to selection criteria")
        sys.exit(-1)

    return prod, download_dict, storage_dict, size_dict


def peps_downloader(options):
    """The main script to download image from peps.

    Args:
        options (ParserConfig): the config object.
    """
    # Set logging
    if options.log_dir is not None:
        log = "{}/{}/peps_download_{}.log" \
            .format(options.dst_dir,
                    options.log_dir,
                    datetime.now().strftime("%d%m%Y_%H%M"))
    else:
        log = "{}/peps_download_{}.log" \
            .format(options.dst_dir,
                    datetime.now().strftime("%d%m%Y_%H%M"))
    # Set up logger
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    log_format = "%(asctime)s::%(levelname)s::%(name)s::%(filename)s::%(lineno)d::%(message)s"
    logging.basicConfig(filename=log, filemode='w',
                        level=logging.INFO, format=log_format)
    logger = logging.getLogger(__name__)

    # Check destination path
    if options.dst_dir is None:
        logger.error("peps_downloader: must set a destination path for results.")
        sys.exit("peps_downloader: must set a destination path for results.")
    if not exists(options.write_dir):
        os.mkdir(options.write_dir)

    # Initialize json file for searching
    if options.search_json_file is None or options.search_json_file == "":
        options.search_json_file = 'search.json'

    if options.sat is not None:
        logger.info("{} {}".format(options.sat, options.collection[0:2]))
        if not options.sat.startswith(options.collection[0:2]):
            print("Input parameters collection and satellite are incompatible")
            logger.error("Input parameters collection and satellite are incompatible")
            sys.exit(-1)

    # Define location for searching: location, point or rectangle
    if options.tile is None:
        if options.geojson is None:
            if options.location is None:
                if options.lat is None or options.lon is None:
                    if options.latmin is None or options.lonmin is None or \
                            options.latmax is None or options.lonmax is None:
                        print("Provide at least tile, location, coordinates, rectangle, or geojson")
                        logger.error("Provide at least tile, location, coordinates, rectangle, or geojson")
                        sys.exit(-1)
                    else:
                        geom = 'rectangle'
                else:
                    if options.latmin is None and options.lonmin is None and \
                            options.latmax is None and options.lonmax is None:
                        geom = 'point'
                    else:
                        print("Please choose between coordinates and rectangle, but not both")
                        logger.error("Please choose between coordinates and rectangle, but not both")
                        sys.exit(-1)
            else:
                if options.latmin is None and options.lonmin is None and \
                        options.latmax is None and options.lonmax is None and \
                        options.lat is None or options.lon is None:
                    geom = 'location'
                else:
                    print("Please choose location and coordinates, but not both")
                    logger.error("Please choose location and coordinates, but not both")
                    sys.exit(-1)
        else:
            if options.latmin is None and options.lonmin is None and \
                    options.latmax is None and options.lonmax is None and \
                    options.lat is None or options.lon is None and \
                    options.location is None:
                geom = 'geojson'
            else:
                print("Please choose location, coordinates, rectangle, or geojson, but not all")
                logger.error("Please choose location, coordinates, rectangle, or geojson, but not all")
                sys.exit(-1)

    # Generate query based on geometric parameters of catalog request
    if options.tile is not None:
        if options.tile.startswith('T') and len(options.tile) == 6:
            tileid = options.tile[1:6]
        elif len(options.tile) == 5:
            tileid = options.tile[0:5]
        else:
            print("Tile name is ill-formatted : 31TCJ or T31TCJ are allowed")
            logger.error("Tile name is ill-formatted : 31TCJ or T31TCJ are allowed")
            sys.exit(-4)
        query_geom = "tileid={}".format(tileid)
    elif geom == 'geojson':
        with open(options.geojson) as f:
            gj = geojson.load(f)
        if len(gj['features']) > 1:
            query_geom = list(map(lambda each: GeoJSON(each).bbox(), gj['features']))
        else:
            bbox_gj = GeoJSON(gj).bbox()
            latmin = bbox_gj[1]
            latmax = bbox_gj[3]
            lonmin = bbox_gj[0]
            lonmax = bbox_gj[2]
            query_geom = 'box={lonmin},{latmin},{lonmax},{latmax}'.format(
                latmin=latmin, latmax=latmax,
                lonmin=lonmin, lonmax=lonmax)
    elif geom == 'point':
        query_geom = 'lat={}\&lon={}'.format(options.lat, options.lon)
    elif geom == 'rectangle':
        query_geom = 'box={lonmin},{latmin},{lonmax},{latmax}'.format(
            latmin=options.latmin, latmax=options.latmax,
            lonmin=options.lonmin, lonmax=options.lonmax)
    elif geom == 'location':
        query_geom = "q={}".format(options.location)

    # date parameters of catalog request
    if options.start_date is not None:
        start_date = options.start_date
        if options.end_date is not None:
            end_date = options.end_date
        else:
            end_date = date.today().isoformat()

    # special case for Sentinel-2
    if options.collection == 'S2':
        if datetime.strptime(options.start_date, '%Y-%m-%d').date() >= \
                datetime.strptime('2016-12-05', '%Y-%m-%d').date():
            print("**** Products after '2016-12-05' are stored in Tiled products collection")
            print("**** Please use option -c S2ST")
            logger.warning("Option -c S2ST should be used for sentinel-2 imagery after '2016-12-05'")
            time.sleep(5)
        elif datetime.strptime(options.end_date, '%Y-%m-%d').date() >= \
                datetime.strptime('2016-12-05', '%Y-%m-%d').date():
            print("**** Products after '2016-12-05' are stored in Tiled products collection")
            print("**** Please use option -c S2ST to get the products after that date")
            print("**** Products before that date will be downloaded")
            logger.warning("Option -c S2ST should be used for sentinel-2 imagery after '2016-12-05'. "
                           "Products before that date will be downloaded")
            time.sleep(5)

    if options.collection == 'S2ST':
        if datetime.strptime(options.end_date, '%Y-%m-%d').date() < \
                datetime.strptime('2016-12-05', '%Y-%m-%d').date():
            print("**** Products before '2016-12-05' are stored in non-tiled products collection")
            print("**** Please use option -c S2")
            logger.warning("Option -c S2 should be used for sentinel-2 imagery before '2016-12-05'")
            time.sleep(5)
        elif datetime.strptime(options.start_date, '%Y-%m-%d').date() < \
                datetime.strptime('2016-12-05', '%Y-%m-%d').date():
            print("**** Products before '2016-12-05' are stored in non-tiled products collection")
            print("**** Please use option -c S2 to get the products before that date")
            print("**** Products after that date will be downloaded")
            logger.warning("Option -c S2 should be used for sentinel-2 imagery before '2016-12-05'. "
                           "Products after that date will be downloaded")
            time.sleep(5)

    # ====================
    # read authentication file
    # ====================
    config = parse_config(options.auth)
    email = config['peps']['user']
    passwd = config['peps']['password']
    if email is None or passwd is None:
        print("Not valid email or passwd for peps.")
        logger.error("Not valid email or passwd for peps.")
        sys.exit(-1)

    # ====================
    # search in catalog
    # ====================
    # Clean search json file
    if os.path.exists(options.search_json_file):
        os.remove(options.search_json_file)

    # Parse catalog
    _query_catalog(options, query_geom, start_date, end_date, logger)

    # Read catalog
    prod, download_dict, storage_dict, size_dict = parse_catalog(options, logger)

    # ====================
    # Download
    # ====================

    if len(download_dict) == 0:
        logger.warning("No product matches the criteria")
    else:
        # first try for the products on tape
        if options.write_dir is None:
            options.write_dir = os.getcwd()

        for prod in list(download_dict.keys()):
            file_exists = os.path.exists("{}/{}.SAFE".format(options.write_dir, prod)) or \
                          os.path.exists("{}/{}.zip".format(options.write_dir, prod))
            if not options.no_download and not file_exists:
                if storage_dict[prod] == "tape":
                    tmticks = time.time()
                    tmpfile = "{}/tmp_{}.tmp".format(options.write_dir, tmticks)
                    logger.info("Stage tape product: {}".format(prod))
                    get_product = "curl -o {} -k -u {}:{} https://peps.cnes.fr/resto/" \
                                  "collections/{}/{}/download" \
                                  "/?issuerId=peps &>/dev/null" \
                        .format(tmpfile, email, passwd,
                                options.collection, download_dict[prod])
                    os.system(get_product)
                    if os.path.exists(tmpfile):
                        os.remove(tmpfile)

        NbProdsToDownload = len(list(download_dict.keys()))
        logger.info("{}  products to download".format(NbProdsToDownload))
        while NbProdsToDownload > 0:
            # redo catalog search to update disk/tape status
            logger.info("Redo catalog search to update disk/tape status.")
            _query_catalog(options, query_geom, start_date, end_date, logger)
            prod, download_dict, storage_dict, size_dict = parse_catalog(options, logger)

            NbProdsToDownload = 0
            # download all products on disk
            for prod in list(download_dict.keys()):
                file_exists = os.path.exists("{}/{}.SAFE".format(options.write_dir, prod)) or \
                              os.path.exists("{}/{}.zip".format(options.write_dir, prod))
                if not options.no_download and not file_exists:
                    if storage_dict[prod] == "disk":
                        tmticks = time.time()
                        tmpfile = "{}/tmp_{}.tmp".format(options.write_dir, tmticks)
                        logger.info("Download of product : {}".format(prod))
                        get_product = "curl -o {} -k -u {}:{} https://peps.cnes.fr/resto" \
                                      "/collections/{}/{}/download" \
                                      "/?issuerId=peps" \
                            .format(tmpfile, email, passwd,
                                    options.collection, download_dict[prod])
                        # print(get_product)
                        os.system(get_product)
                        # check binary product, rename tmp file
                        if not os.path.exists("{}/tmp_{}.tmp".format(options.write_dir, tmticks)):
                            NbProdsToDownload += 1
                        else:
                            check_rename(tmpfile, options, prod, size_dict[prod], logger)

                elif file_exists:
                    logger.info("{} already exists".format(prod))

            # download all products on tape
            for prod in list(download_dict.keys()):
                file_exists = os.path.exists("{}/{}.SAFE".format(options.write_dir, prod)) or \
                              os.path.exists("{}/{}.zip".format(options.write_dir, prod))
                if not options.no_download and not file_exists:
                    if storage_dict[prod] == "tape" or storage_dict[prod] == "staging":
                        NbProdsToDownload += 1

            if NbProdsToDownload > 0:
                logger.info("{} remaining products are on tape, let's wait 1 minutes before trying again"
                            .format(NbProdsToDownload))
                time.sleep(60)


def peps_maja_process(start_date, end_date, tile,
                      log_name, email, passwd, logger,
                      no_download=False, orbit=None):
    """The main script to precess image by maja based on tile.

    Args:
        start_date (str): start date to process.
        end_date (str): end date to process.
        tile (str): tile name.
        log_name (str): log file name.
        email (str): peps email.
        passwd (str): peps password.
        no_download (bool): True if not download, otherwise False.
        orbit (int): orbit number.
        logger (logging.Logger): logger object to store logs.
    Returns:
        bool: True if success, otherwise False.
    """
    # Format tile
    if tile.startswith('T'):
        tile = tile[1:]

    # Check params
    check_params(start_date, end_date, tile, orbit)

    # =====================
    # Start Maja processing
    # =====================
    peps = "http://peps.cnes.fr/resto/wps"
    if orbit is not None:
        url = "{}?request=execute&service=WPS&version=1.0.0&identifier" \
              "=FULL_MAJA&datainputs=startDate={};completionDate={};tileid={};" \
              "relativeOrbitNumber={}&status=true&storeExecuteResponse=true" \
            .format(peps, start_date, end_date, tile, orbit)
    else:
        url = "{}?request=execute&service=WPS&version=1.0.0&identifier" \
              "=FULL_MAJA&datainputs=startDate={};completionDate={};" \
              "tileid={}&status=true&storeExecuteResponse=true" \
            .format(peps, start_date, end_date, tile)
    logger.info(url)
    if not no_download:
        req = requests.get(url, auth=(email, passwd))
        with open(log_name, "wb") as f:
            f.write(req.text.encode('utf-8'))
        if req.status_code == 200:
            if b"Process FULL_MAJA accepted" in req.text.encode('utf-8'):
                logger.info("Request OK ! log is in {}".format(log_name))
                return True
            else:
                logger.info("Something is wrong : please check {} file".format(log_name))
                return False
        elif req.status_code == 401:
            logger.info("Unauthorized request, please check the auth file with provided -a option")
            return False
        else:
            logger.info("Wrong request status {}".format(str(req.status_code)))
            return False


def peps_maja_downloader(write_dir, email, password, log_name, logger):
    """The main script to precess image by maja based on tile.

    Args:
        write_dir (str): dir to save results.
        email (str): peps email.
        password (str): peps password.
        log_name (str): log file name.
        logger (logging.Logger): logger object to store logs.
    Returns:
        bool: True if download, otherwise False.
    """
    if not (os.path.exists(write_dir)):
        os.mkdir(write_dir)

    try:
        with open(log_name) as f:
            lignes = f.readlines()
            urlStatus = None
            for ligne in lignes:
                if ligne.startswith("<wps:ExecuteResponse"):
                    wpsId = ligne.split("pywps-")[1].split(".xml")[0]
                    urlStatus = "https://peps.cnes.fr/cgi-bin/mapcache_results/logs/joblog-{}.log".format(wpsId)
            if urlStatus is None:
                logger.error("url for production status not found in logName %s" % log_name)
                sys.exit(-4)
    except IOError:
        logger.error("error with logName file provided as input or as default parameter")
        sys.exit(-3)

    statusFileName = log_name.replace('log', 'stat')
    if not (os.path.exists(os.path.dirname(statusFileName))):
        os.mkdir(write_dir)
    getURL(urlStatus, statusFileName, email, password, logger)

    urls = []
    try:
        with open(statusFileName) as f:
            lignes = f.readlines()
            for ligne in lignes:
                if ligne.find("https://peps.cnes.fr/cgi-bin/mapcache_results/maja/{}".format(wpsId)) >= 0:
                    url = re.search('https:(.+).zip', ligne).group(0)
                    urls.append(url)
    except IOError:
        logger.error("Rrror with status url found in logName")
        sys.exit(-3)

    if len(urls) > 0:
        for url in urls:
            L2AName = url.split('/')[-1]
            if L2AName.find('NOVALD') >= 0:
                logger.info("%s was too cloudy" % L2AName)
            elif os.path.isfile(os.path.join(write_dir, L2AName)):
                logger.info("Skipping {}: already on disk".format(L2AName))
            else:
                logger.info("downloading %s" % L2AName)
                downloadFile(url, "%s/%s" % (write_dir, L2AName), email, password)
        return True
    else:
        return False


def s2_maja_process(options, stage=False):
    """Run maja installed on peps server
    to process sentinel-2 imagery

    Args:
        options (ParserConfig): the ParserConfig for options
        stage (bool): if stage images first.
    Returns:
        bool: True if request success, otherwise False.
    """
    # Set logging
    if options.log_dir is not None:
        log = "{}/{}/full_maja_process_{}.log" \
            .format(options.dst_dir,
                    options.log_dir,
                    datetime.now().strftime("%d%m%Y_%H%M"))
    else:
        log = "{}/full_maja_process_{}.log" \
            .format(options.dst_dir,
                    datetime.now().strftime("%d%m%Y_%H%M"))

    # Set up logger
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    log_format = "%(asctime)s::%(levelname)s::%(name)s::%(filename)s::%(lineno)d::%(message)s"
    logging.basicConfig(filename=log, filemode='w',
                        level=logging.INFO, format=log_format)
    logger = logging.getLogger(__name__)

    # Check destination path
    if options.dst_dir is None:
        logger.error("full_maja_process: must set a destination path for results.")
        sys.exit("full_maja_process: must set a destination path for results.")

    # ====================
    # read authentication file
    # ====================
    config = parse_config(options.auth)
    email = config['peps']['user']
    passwd = config['peps']['password']
    if email is None or passwd is None:
        print("Not valid email or passwd for peps.")
        logger.error("full_maja_process: Not valid email or passwd for peps.")
        sys.exit(-1)

    # Check conditions on dates
    # Date parameters of catalog request
    if options.start_date is not None:
        start_date = options.start_date
        if options.end_date is not None:
            end_date = options.end_date
        else:
            end_date = datetime.date.today().isoformat()
    sdate = datetime.strptime(start_date, '%Y-%m-%d')
    edate = datetime.strptime(end_date, '%Y-%m-%d')
    if edate < datetime.strptime('2016-04-01', '%Y-%m-%d'):
        logger.error("full_maja_process: Because of missing information on ESA L1C products, "
                     "start_date must be greater than '2016-04-01'")
        sys.exit("Error with end date.")

    if (edate - sdate).days < 50:
        logger.error("full_maja_process: At least 50 days should be provided to "
                     "MAJA to allow a proper initialisation")
        sys.exit("Time interval is too small.")

    if (edate - sdate).days > 366:
        logger.error("full_maja_process: Due to processing and disk limitations, "
                     "processing is limited to a one year period per command line")
        sys.exit("Time interval is too large.")

    # Stage images to disk and get catalog
    no_download_val = options.no_download
    options.no_download = True
    if stage:
        while True:
            logger.info("full_maja_process: Stage imagery.")
            peps_downloader(options)
            prod, download_dict, storage_dict, size_dict = parse_catalog(options, logger)
            # Check
            if list(set(storage_dict.values())) == ['disk']:
                logger.info("full_maja_process: All images are on disk.")
                break

            # Stage
            for prod in list(download_dict.keys()):
                if storage_dict[prod] == "tape":
                    tmticks = time.time()
                    tmpfile = "{}/tmp_{}.tmp".format(options.write_dir, tmticks)
                    logger.info("full_maja_process: Stage tape product: {}".format(prod))
                    get_product = "curl -o {} -k -u {}:{} https://peps.cnes.fr/resto/" \
                                  "collections/{}/{}/download" \
                                  "/?issuerId=peps &>/dev/null" \
                        .format(tmpfile, email, passwd,
                                options.collection, download_dict[prod])
                    os.system(get_product)
                    if os.path.exists(tmpfile):
                        os.remove(tmpfile)
    else:
        logger.info("full_maja_process: No stage imagery.")
        peps_downloader(options)
    options.no_download = no_download_val
    prod, download_dict, storage_dict, size_dict = parse_catalog(options, logger)
    prod = list(set(download_dict.keys()))
    tiles_dup = list(map(lambda x: re.search("T[0-9]{2}[A-Z]{3}", x).group(0), prod))
    tiles = list(set(map(lambda x: re.search("T[0-9]{2}[A-Z]{3}", x).group(0), prod)))
    tiles = list(_divide_chunks(tiles, 10))

    # Request maja
    # Create path for logs
    if not os.path.isdir(join(options.dst_dir, options.maja_log)):
        os.mkdir(join(options.dst_dir, options.maja_log))

    for each in tiles:
        tiles_done = []
        wait_lens = []
        for tile in each:
            # Set logName for maja
            log_name = join(options.dst_dir, options.maja_log, '{}.log'.format(tile))
            if peps_maja_process(start_date, end_date, tile,
                                 log_name, email, passwd,
                                 logger=logger, no_download=options.no_download):
                tiles_done.append(tile)
                wait_lens.append(60 + 25 * (tiles_dup.count(tile) - 1))
                logger.info('full_maja_process: query maja for tile {} success.'.format(tile))
            else:
                logger.error('full_maja_process: query maja for tile {} fails.'.format(tile))
        time.sleep(max(wait_lens))

        # Download finished images
        for tile in tiles_done:
            log_name = join(options.dst_dir, options.maja_log, '{}.log'.format(tile))
            while True:
                if peps_maja_downloader(options.processed_dir, email, passwd, log_name, logger):
                    logger.info('full_maja_process: download imagery of tile {} success.'.format(tile))
                    break

    print('Request finish. Please check {} for details.'.format(log))

