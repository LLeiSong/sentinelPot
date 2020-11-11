"""
This is a chunk which includes all steps for 
preprocessing of sentinel-1 GRD or sentinel-2 products.
Author: Lei Song
Maintainer: Lei Song (lsong@clarku.edu)
"""
import sys
import logging
import shutil
import time
import multiprocessing as mp
from datetime import datetime
from os.path import exists
from .sentinel_client import *
from .fixed_thread_pool_executor import FixedThreadPoolExecutor
from .internal_functions import _run_cmd, _unzip_file, _load_yaml
from .peps import ParserConfig, s2_maja_process, peps_downloader


def scihub_downloader(config):
    """Download sentinel imagery from sci-hub.

    Args:
        config (dict): the dictionary of configs.
    """
    # Set up logger
    dst_dir = config['dirs']['dst_dir']
    log_dir = join(dst_dir, config['dirs']['log_dir'])
    if not exists(log_dir):
        os.makedirs(log_dir)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    log_path = join(log_dir, 'sentinel_downloader_{}.log'
                    .format(datetime.now().strftime("%d%m%Y_%H%M")))
    log_format = "%(asctime)s::%(levelname)s::%(name)s::%(filename)s::%(lineno)d::%(message)s"
    logging.basicConfig(filename=log_path, filemode='w',
                        level=logging.INFO, format=log_format)
    logger = logging.getLogger(__name__)

    # Download
    if config['sentinel']['platformname'] in ['S1', 'S2']:
        logger.info("Sentinel_downloader: query sentinel images.")
        sc = SentinelClient(config)
        scenes = sc.get_scenes()
        scene_ids = sc.get_scene_ids(scenes)
        logger.info("Sentinel_downloader: there are {} sentinel tiles".format(len(scene_ids)))

        # make the title list
        logger.info("Sentinel_downloader: make title list.")
        sc.make_footprints()
        logger.info("Sentinel_downloader: save title list to {}.".format(sc.footprint_list))

        # trigger and download imagery recursively
        logger.info("Sentinel_downloader: start to trigger and download imagery recursively.")
        n_loop = 0
        while True:
            # Filter the finished scenes
            logger.info("Sentinel_downloader: filter finished ones.")
            scenes_finished = sc.get_finished_titles()
            scene_ids = []
            for key, item in scenes.items():
                if item['title'] not in scenes_finished:
                    scene_ids.append(key)
            logger.info("Sentinel_downloader: loop {}-there are {} scenes left".format(n_loop, len(scene_ids)))

            # Check left scenes
            if len(scene_ids) == 0:
                logger.info("Sentinel_downloader: finish downloading all scenes.")
                break

            # start a new loop to download
            online_count = 0
            offline_count = 0
            trigger_count = 0
            for scene_id in scene_ids:
                product_info = sc.api.get_product_odata(scene_id)
                if product_info['Online']:
                    online_count = online_count + 1
                    sc.download_one(scene_id)
                else:
                    offline_count = offline_count + 1
                    while True:
                        with sc.api.session.get(product_info["url"],
                                                auth=sc.api.session.auth,
                                                timeout=sc.api.timeout) as r:
                            status = r.status_code
                            if status == 202:
                                sc.download_one(scene_id)
                                trigger_count = trigger_count + 1
                                break
                            if status == 403:
                                time.sleep(600)
                logger.info("Sentinel_downloader: loop {}, {} out of {} are processed."
                            .format(n_loop, online_count + offline_count, len(scene_ids)))
                logger.info("Sentinel_downloader: loop {}, {} imagery are triggered."
                            .format(n_loop, trigger_count))
            logger.info("Sentinel_downloader: loop {}, there are {} online imagery, "
                        "and {} are triggered from {} offline imagery."
                        .format(n_loop, online_count, trigger_count, offline_count))
            logger.info("Sentinel_downloader: Finished loop {}.".format(n_loop))
            n_loop = n_loop + 1
            time.sleep(7200)
    else:
        logger.error("Sentinel_downloader: not support platform, [S1, S2].")
    print('s_download is done, please check {} for logs.'.format(log_path))


def s1_gpt_process(fname, gpt_path,
                   download_path, processed_path,
                   xml_path='./files/S1_GRD_preprocessing.xml',
                   logger=None):
    """Preprocess sentinel using gpt

    Args:
        processed_path (str): path to store the processed files.
        download_path (str): path to download the files.
        fname (str): the name of file.
        gpt_path (str): the path of gpt, should be `{snap_dir}/etc/bin/gpt`.
        xml_path (str): path of xml file.
        logger (logging.Logger): the logger object to store logs.
    """
    fname_safe = fname.replace("zip", "SAFE")
    fname_out = fname.replace("zip", "dim")
    fname_input = f"{download_path}/{fname_safe}"
    fname_output = f"{processed_path}/{fname_out}"

    # Run cmd
    if not exists(fname_output):
        # Unzip
        if not exists(fname_input):
            _unzip_file(fname, download_path)

        cmd = f"{gpt_path} {xml_path} -Presolution=10 -Porigin=5" + \
              f" -Pfilter='Refined Lee' -Pdem='SRTM 3Sec'" + \
              f" -Pcrs='GEOGCS[\"WGS84(DD)\", DATUM[\"WGS84\"," + \
              f" SPHEROID[\"WGS84\", 6378137.0, 298.257223563]]," + \
              f" PRIMEM[\"Greenwich\", 0.0], UNIT[\"degree\", 0.017453292519943295]," + \
              f" AXIS[\"Geodetic longitude\", EAST], AXIS[\"Geodetic latitude\", NORTH]]'" + \
              f" -Pinput={fname_input} -Poutput={fname_output}"
        _run_cmd(cmd, logger)

        # Remove unzipped file
        if exists(fname_input):
            shutil.rmtree(fname_input)
        logger.info("s1_gpt_process: preprocess done, saved as {}.".format(fname_output))
    else:
        logger.info("s1_gpt_process: {} already exists.".format(fname_output))


def s1_preprocess(config_path, query=False, source='peps', parallel=False):
    """The script to batch preprocess sentinel-1 GRD products stored in a folder.
    To run it correctly, config yaml needs to be set right.

    Args:
        config_path (str): the path of config yaml.
        query (bool): option to query imagery first.
        source (str): the source to download, ['peps', 'scihub']
        parallel (bool): option to process in parallel.
    """
    config = _load_yaml(config_path)
    # Set destination directory for working
    # Create folders if necessary
    dst_dir = config['dirs']['dst_dir']
    log_dir = join(dst_dir, config['dirs']['log_dir'])
    catalogs_dir = join(dst_dir, 'catalogs')
    download_path = join(dst_dir, config['dirs']['download_path'])
    processed_path = join(dst_dir, config['dirs']['processed_path'])
    for d in [log_dir, catalogs_dir, download_path, processed_path]:
        if not exists(d):
            os.makedirs(d)

    # Set up logger
    # The log file with suffix %d%m%Y_%H%M
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    log_path = join(log_dir, 'sentinel1_preprocess_{}.log'
                    .format(datetime.now().strftime("%d%m%Y_%H%M")))
    log_format = "%(asctime)s::%(levelname)s::%(name)s::%(filename)s::%(lineno)d::%(message)s"
    logging.basicConfig(filename=log_path, filemode='w',
                        level=logging.INFO, format=log_format)
    logger = logging.getLogger(__name__)

    # Start pre-processing
    logger.info("Sentinel1_preprocess: start pre-processing sentinel-1 images.")
    # If query data first
    if query:
        # Query scenes
        logger.info("Sentinel1_preprocess: query sentinel-1 images.")
        if source == 'peps':
            logger.info("Sentinel1_preprocess: query sentinel-1 images from peps server.")
            options = ParserConfig(config_path)
            peps_downloader(options)
        elif source == 'scihub':
            logger.info("Sentinel1_preprocess: query sentinel-1 images from sentinel hub.")
            scihub_downloader(config)
        else:
            logger.error("Sentinel1_preprocess: no such source.")
            sys.exit("Sentinel1_preprocess: no such source.")

    # preprocess imagery
    # Set config paths of GPT
    logger.info("Sentinel1_preprocess: parse GPT config variables.")
    gpt_path = config['gpt']['gpt_path']
    if config['gpt']['xml_path'] is None:
        c_dir = os.path.dirname(__file__)
        xml_path = join(c_dir, 'files/S1_GRD_preprocessing.xml')
    else:
        xml_path = config['gpt']['xml_path']
    if not exists(xml_path):
        logger.error("Sentinel1_preprocess: xml file {} not found.".format(xml_path))
        sys.exit("Sentinel1_preprocess: xml file {} not found.".format(xml_path))
    logger.info("Sentinel1_preprocess: gpt is set to use {}.".format(gpt_path))
    logger.info("Sentinel1_preprocess: xml is set to use {}.".format(xml_path))

    # Get files
    fnames = os.listdir(download_path)
    fnames = list(filter(lambda fn: ".zip" in fn, fnames))

    if parallel:
        # Process with fixed threads
        # determine thread number to be used
        threads_number = config['parallel']['threads_number']
        if threads_number == 'default':
            threads_number = mp.cpu_count()
        else:
            threads_number = int(threads_number)
        logger.info("Sentinel1_preprocess: set number of threads to {}.".format(threads_number))

        success_count = 0
        failure_count = 0
        s1_process_executor = FixedThreadPoolExecutor(size=threads_number)
        for fname in fnames:
            if s1_process_executor.submit(s1_gpt_process, fname, gpt_path,
                                          download_path, processed_path,
                                          xml_path, logger) is True:
                success_count += 1
            else:
                failure_count += 1
        # await all tile finished
        s1_process_executor.drain()
        # await thread pool to stop
        s1_process_executor.close()
        logger.info("Sentinel1_preprocess: finished tasks; the total file number to be processed is {}; "
                    "the success_count is {}; the failure_count is {}"
                    .format(len(fnames), success_count, failure_count))
    else:
        # Process one by one
        success_count = 0
        failure_count = 0
        for fname in fnames:
            if s1_gpt_process(fname, gpt_path, download_path,
                              processed_path, xml_path, logger) is True:
                success_count += 1
            else:
                failure_count += 1
        logger.info("Sentinel1_preprocess: finished tasks; the total file number to be processed is {}; "
                    "the success_count is {}; the failure_count is {}"
                    .format(len(fnames), success_count, failure_count))
    print('s1_preprocess is done, please check {} for logs.'.format(log_path))


def s2_atmospheric_correction(fname, processed_path='./sen2cor',
                              sen2cor_path=None, logger=None):
    """Do atmospheric correction using sen2cor for sentinel2 imagery

    Args:
        fname (str): the full path of sentinel-2 SAFE.
        processed_path (str): the directory for results.
        sen2cor_path (str): the path of sen2cor.
        logger (logging.Logger): the logging object to store logs.
    """

    # Run sen2cor
    if sen2cor_path is None or (not exists(sen2cor_path)):
        logger.error('sen2cor_path is not set correctly.')
        exit('sen2cor_path is not set correctly.')
    else:
        if not exists(processed_path):
            os.mkdir(processed_path)
        cmd = '{} --output_dir {} {}'.format(sen2cor_path, processed_path, fname)
        _run_cmd(cmd, logger)


def s2_fmask(tile_name, config, logger):
    """Do cloud and shadow detection using fmask for sentinel2 imagery

    Args:
        tile_name (str): the name of sentinel-2 tile.
        logger (logging.Logger): the logger object to store logs.
        config (dict): the dictionary of parameters for fmask.
    """
    # Set paths
    download_path = join(config['dirs']['dst_dir'],
                         config['dirs']['download_path'])
    processed_path = join(config['dirs']['dst_dir'],
                          config['dirs']['processed_path'])
    docker = config['fmask']['docker']

    # Check paths
    if download_path is None:
        logger.error('Did not set path of download path.')
        exit('Did not set path of download path.')
    if not exists(processed_path):
        os.mkdir(processed_path)
        logger.info('Create processed path {}.'.format(processed_path))

    # Format tile_id
    tile_id = tile_name.split("_")[5]
    tile_dt = tile_name.split("_")[2]
    fnames_all = os.listdir(processed_path)
    # Because of a bug in output folder name of sen2cor
    safe_path = list(filter(lambda fname: tile_id in fname and tile_dt in fname, fnames_all))
    if len(safe_path) == 1:
        dst_path = join(processed_path, safe_path[0], "GRANULE")
        img_path = list(filter(lambda fname: "L2A" in fname, os.listdir(dst_path)))
        mv_path = join(dst_path, img_path[0], "FMASK_DATA/")
        os.makedirs(mv_path)
        src_path = join(dst_path, img_path[0].replace("L2A", "L1C"), "*.tif")
        del_path = join(dst_path, img_path[0].replace("L2A", "L1C"))
    else:
        if logger is None:
            print("There is no atmospheric corrected tile for ", tile_name)
        else:
            logger.warning("There is no atmospheric corrected tile for ", tile_name)
        exit("There is no atmospheric corrected tile for ", tile_name)

    if docker:
        # Check docker
        cmd = 'docker image ls | grep "fmask"'
        if not _run_cmd(cmd, logger):
            logger.error('There is no docker image fmask running. Please use fmask_docker_install to install.')
            exit('There is no docker image fmask running. Please use fmask_docker_install to install.')
        # Run Fmask
        # get parameters
        cloudprobthreshold = config['fmask']['cloudprobthreshold']
        cloudbufferdistance = config['fmask']['cloudbufferdistance']
        shadowbufferdistance = config['fmask']['shadowbufferdistance']
        cmd = 'docker run -v {}:/mnt/input-dir:ro -v ' \
              '{}:/mnt/output-dir:rw fmask ' \
              '{} *{}* {} {} 0 {}; mv {} {}'.format(download_path, dst_path,
                                                    tile_name, tile_id,
                                                    cloudbufferdistance,
                                                    shadowbufferdistance,
                                                    cloudprobthreshold,
                                                    src_path, mv_path)
        if _run_cmd(cmd, logger):
            if exists(del_path):
                shutil.rmtree(del_path)
            return True
        else:
            return False
    else:
        # Set paths
        # target_path
        fmask_path = config['fmask']['fmask_path']
        mc_root = config['fmask']['mc_root']
        granule_path = "{}/{}.SAFE/GRANULE".format(download_path, tile_name)
        work_path = join(granule_path, os.listdir(granule_path)[0])
        src_path = join(work_path, "FMASK_DATA", "*.tif")

        # Run Fmask
        # get parameters
        cloudprobthreshold = config['fmask']['cloudprobthreshold']
        cloudbufferdistance = config['fmask']['cloudbufferdistance']
        shadowbufferdistance = config['fmask']['shadowbufferdistance']
        cmd = 'cd {}; {} {} {} {} 0 {}; mv {} {}'.format(work_path,
                                                         fmask_path, mc_root,
                                                         cloudbufferdistance,
                                                         shadowbufferdistance,
                                                         cloudprobthreshold,
                                                         src_path, mv_path)
        if _run_cmd(cmd, logger):
            if exists(del_path):
                shutil.rmtree(del_path)
            return True
        else:
            return False


def s2_preprocess_each(tile_name, config, logger):
    """Preprocess for sentinel2 imagery
    This function use sen2cor and fmask together.

    Args:
        tile_name (str): the name of sentinel-2 tile.
        config (dict): config dictionary.
        logger (logging.Logger): the logger object to store logs.
    """
    # File name
    download_path = join(config['dirs']['dst_dir'],
                         config['dirs']['download_path'])
    fname = join(download_path, "{}.SAFE".format(tile_name))
    keep = True
    if not exists(fname):
        fname_zip = fname.replace('SAFE', 'zip')
        if exists(fname_zip):
            keep = False
            _unzip_file(fname_zip, download_path)
        else:
            logger.error("s2_preprocess_each: No zip or SAFE file found for {}.".format(tile_name))
            exit("s2_preprocess_each: No zip or SAFE file found for {}.".format(tile_name))

    # sen2cor
    # Set paths
    processed_path = join(config['dirs']['dst_dir'],
                          config['dirs']['processed_path'])
    sen2cor_path = config['sen2cor']['sen2cor_path']
    if not s2_atmospheric_correction(fname, processed_path, sen2cor_path, logger):
        logger.error("s2_preprocess_each: Atmospheric correction for tile {} failed.".format(tile_name))
        exit("s2_preprocess_each: Atmospheric correction for tile {} failed.".format(tile_name))
    logger.info("s2_preprocess_each: Atmospheric correction for tile {} done.".format(tile_name))

    # Fmask
    if not s2_fmask(tile_name, config, logger):
        logger.error("s2_preprocess_each: Fmask for tile {} failed.".format(tile_name))
        exit("s2_preprocess_each: Fmask for tile {} failed.".format(tile_name))
    logger.info("s2_preprocess_each: Fmask calculation for tile {} done.".format(tile_name))

    # Remove unzipped files
    if not keep:
        shutil.rmtree(fname)


def s2_preprocess(config_path, option='regular', query=False, source='peps'):
    """The script to batch preprocess sentinel-2 products stored in a folder.
    It uses sen2cor and fmask.
    To run it correctly, config yaml needs to be set right.

    Args:
        config_path (str): the path of config yaml.
        option (str): option to preprocess, maja or regular.
        query (bool): option to query imagery first.
        source (str): the source to download, ['peps', 'scihub']
    """

    config = _load_yaml(config_path)
    # Set destination directory for working
    # Create folders if necessary
    dst_dir = config['dirs']['dst_dir']
    log_dir = join(dst_dir, config['dirs']['log_dir'])
    catalogs_dir = join(dst_dir, 'catalogs')
    download_path = join(dst_dir, config['dirs']['download_path'])
    processed_path = join(dst_dir, config['dirs']['processed_path'])
    for d in [log_dir, catalogs_dir, download_path, processed_path]:
        if not exists(d):
            os.makedirs(d)

    # Set up logger
    # The log file with suffix %d%m%Y_%H%M
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    log_path = join(log_dir, 'sentinel2_preprocess_{}.log'
                    .format(datetime.now().strftime("%d%m%Y_%H%M")))
    log_format = "%(asctime)s::%(levelname)s::%(name)s::%(filename)s::%(lineno)d::%(message)s"
    logging.basicConfig(filename=log_path, filemode='w',
                        level=logging.INFO, format=log_format)
    logger = logging.getLogger(__name__)

    # preprocess imagery
    if option == 'regular':
        # If query
        if query:
            # Query scenes
            logger.info("Sentinel1_preprocess: query sentinel-1 images.")
            if source == 'peps':
                logger.info("Sentinel1_preprocess: query sentinel-1 images from peps server.")
                peps_downloader(config)
            elif source == 'scihub':
                logger.info("Sentinel1_preprocess: query sentinel-1 images from sentinel hub.")
                scihub_downloader(config)
            else:
                logger.error("Sentinel1_preprocess: no such source.")
                sys.exit("Sentinel1_preprocess: no such source.")

        # Get scenes
        sc = SentinelClient(config)
        # Filter the finished scenes
        if os.path.isfile(sc.footprint_list):
            s2_footprints = read_geojson(sc.footprint_list)
        else:
            s2_footprints = sc.make_footprints()
        logger.info('Sentinel2_preprocess: get footprints.')

        # Get tiles to process
        tile_names = []
        for feature in s2_footprints['features']:
            title = feature['properties']['title']
            # title = re.sub("N[0-9]{4}", "N9999", re.sub("L1C", "L2A", title))
            # title = re.sub("_[0-9]{8}T[0-9]+$", "", title)
            tile_names.append(title)
        tile_names = list(set(tile_names))
        logger.info('Sentinel2_preprocess: get tiles.')
        logger.info('Sentinel2_preprocess: there are {} tile need to process.'.format(len(tile_names)))

        # determine thread number to be used
        threads_number = config['parallel']['threads_number']
        if threads_number == 'default':
            threads_number = mp.cpu_count()
        else:
            threads_number = int(threads_number)
        logger.info("Sentinel2_preprocess: set number of threads to {}.".format(threads_number))

        # Process with fixed threads
        success_count = 0
        failure_count = 0
        s2_preprocess_executor = FixedThreadPoolExecutor(size=threads_number)
        for tile_name in tile_names:
            if s2_preprocess_executor.submit(s2_preprocess_each, tile_name,
                                             config, logger) is True:
                success_count += 1
            else:
                failure_count += 1
        # await all tile finished
        s2_preprocess_executor.drain()
        # await thread pool to stop
        s2_preprocess_executor.close()
        logger.info("Sentinel2_preprocess: finished s2 preprocess task in regular mode; "
                    "the total tile number to be processed is {}; "
                    "the success_count is {}; the failure_count is {}"
                    .format(len(tile_names), success_count, failure_count))
    elif option == 'maja':
        options = ParserConfig(config_path)
        s2_maja_process(options)
    else:
        logger.error('Wrong process mode.')

    print('s2_preprocess is done, please check {} for logs.'.format(log_path))
