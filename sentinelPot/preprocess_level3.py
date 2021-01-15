"""
This is a chunk which includes all steps for 
harmonic regression of sentinel-1 GRD.
Author: Lei Song and Boka Luo
Maintainer: Lei Song (lsong@clarku.edu)
"""
import copy
import logging
import math
import os
import re
import shutil
import sys
import multiprocessing as mp
from datetime import datetime
from datetime import timedelta
from os.path import exists
from os.path import join
import gdal
import numpy as np
from rpy2.robjects.packages import STAP
from sklearn.linear_model import Lasso
from .fixed_thread_pool_executor import FixedThreadPoolExecutor
from .guided_filter import guided_filter
from .internal_functions import _run_cmd, _copytree, _load_yaml, _unzip_file
from .sentinel_client import SentinelClient


def guided_filter_batch(tile_index, config, out_format='ENVI', logger=None):
    """Apply guided filter to the imagery within the target folder.

    Args:
        tile_index (str): the index of tile.
        config (dict): config dictionary.
        out_format (str); the format of output.
        Now it only supports [ENVI, GTiff].
        logger (logging.Logger): logging object.
    """
    # Set path
    dir_clip = config['dirs']['dir_clip']
    dir_clip = "{}_{}".format(dir_clip, str(tile_index))
    dir_ard = config['dirs']['dir_ard']
    dir_ard = "{}_{}".format(dir_ard, str(tile_index))
    if not os.path.isdir(dir_clip):
        if logger is None:
            logger.info("No clip generated for tile {}".format(tile_index))
        else:
            print("No clip generated for tile {}".format(tile_index))
        sys.exit("No clip generated for tile {}".format(tile_index))
    if not os.path.isdir(dir_ard):
        os.mkdir(dir_ard)

    # Filter params
    ksize = config['harmonic']['kernel']
    eps = config['harmonic']['eps']

    # Get clipped file names
    fnames = os.listdir(dir_clip)
    fnames = list(filter(lambda fname: ".img" in fname and ".aux.xml" not in fname, fnames))
    fnames = list(map(lambda fname: os.path.join(dir_clip, fname), fnames))

    # determine thread number to be used
    threads_number = config['parallel']['threads_number']
    if threads_number == 'default':
        threads_number = mp.cpu_count()
    else:
        threads_number = int(threads_number)

    # pool = mp.Pool(processes=threads_number)
    # guided_filter_partial = partial(guided_filter, ksize=ksize, eps=eps, dst_dir=dir_ard)
    # pool.map(guided_filter_partial, fnames)
    # pool.close()
    gf_executor = FixedThreadPoolExecutor(size=threads_number)
    for src_path in fnames:
        gf_executor.submit(guided_filter, src_path, ksize, eps, dir_ard, out_format)
    gf_executor.drain()
    gf_executor.close()


def _get_date(fname):
    """Get date from a filename.
    Wrote and modified with Boka Luo

    Args:
        fname (str): the name of file.

    Returns:
        datetime.date: the date.
    """
    date = fname.split("_")[4]
    date = datetime.strptime("-".join([date[0:4], date[4:6], date[6:8]]), '%Y-%m-%d')
    return date


def _sort_fnames(fnames):
    """Sort fnames based on date.
    
    Args:
        fnames (list of str): a list of fnames.

    Returns:
        list of str: a list of sorted fname.
    """
    dates = list(map(lambda fname: _get_date(fname), fnames))
    fnames = [fnames[i] for i in np.argsort(dates)]
    return fnames


def _get_date_doy(date):
    """Get the day of year from a date.

    Args:
        date (datetime.date): the date.

    Returns:
        int: day of year.
    """
    day_of_year = date.timetuple().tm_yday
    return day_of_year


def _adjust_doy(doys, start, freq):
    """Adjust the day of year based on start day.

    Args:
        doys (numpy.ndarray): an array of DOY.
        start (int): a start DOY.
        freq (int): the frequency of the year, e.g. 365.

    Returns:
        list: an array of adjusted DOYs.
    """
    doys = list(map(lambda doy: doy - start, doys))
    doys = list(map(lambda doy: doy if doy >= 0 else doy + start + freq - start, doys))
    return doys


def _get_doy(fnames, freq):
    """Get the day of year from a filename.

    Args:
        freq (int): the frequency of the year, e.g. 365.
        fnames (list of str): the list of file names.

    Returns:
        numpy.ndarray: an array of DOY.
    """
    # Extract time
    dates = list(map(lambda fname: _get_date(fname), fnames))
    dates.sort()

    # Get DOY
    doys = list(map(lambda date: _get_date_doy(date), dates))

    # Adjust DOY
    doys_check = copy.copy(doys)
    doys_check.sort()
    if not doys_check == doys:
        doys = _adjust_doy(doys, doys[0], freq)
    doys = np.array(doys)
    return doys


def _getVariable(freq, day_arr, n):
    """Generate x variable for harmonic regression.
    Wrote and modified with Boka Luo
    
    Args:
        freq (int): the frequency of the year, e.g. 365.
        day_arr (numpy.ndarray): the array of days.
        n (int): num of harmonic pairs

    Returns:
        numpy.ndarray: x variable for harmonic regression.
    """
    t = np.tile(np.array([day_arr]).transpose(), (1, n))
    n = np.tile(np.arange(n), (len(day_arr), 1))
    n0 = np.ceil(n / 2)

    out = np.cos((2 * np.pi * n0 * t) / freq - (n % 2) * np.pi / 2)
    out[:, 0] = t[:, 0]

    return out


def read_rows(fname, offset_row, n_cols):
    """Read rows of an imagery.
    
    Args:
        fname (str): path of imagery.
        offset_row (int): the offset of row.
        n_cols (int): the col size to read.

    Returns:
        numpy.ndarray: an array of values of target rows.
    """
    img = gdal.Open(fname)
    band = img.GetRasterBand(1)
    novalue = np.float32(band.GetNoDataValue())
    values = band.ReadAsArray(0, offset_row, n_cols, 1)
    values_out = np.where(values == novalue, np.nan, values)
    img = None
    band = None

    return values_out.flatten()


def read_img(fname):
    """Read a full imagery.

    Args:
        fname (str): path of imagery.

    Returns:
        numpy.ndarray: an array of values of target rows.
    """
    img = gdal.Open(fname)
    band = img.GetRasterBand(1)
    novalue = np.float32(band.GetNoDataValue())
    values = band.ReadAsArray()
    values_out = np.where(values == novalue, np.nan, values)
    img = None
    band = None

    return values_out


def harmonic_fitting(tile_index, pol, config):
    """Fit harmonic regression coefficients for a tile.
    
    Args:
        tile_index (str): the index of tile.
        pol (str): polarization mode, VV or VH.
        config (dict): config dictionary.
    """
    # directions
    dir_ard = config['dirs']['dir_ard']
    dir_ard = "{}_{}".format(dir_ard, tile_index)
    dir_coefs = join(config['dirs']['dst_dir'], config['dirs']['dir_coefs'])  # dir_out
    if not os.path.isdir(dir_ard):
        sys.exit("No ARD generated for tile {}".format(tile_index))
    if not os.path.isdir(dir_coefs):
        os.mkdir(dir_coefs)

    # harmonic parameters
    freq = config['harmonic']["harmonic_frequency"]
    num_pair = config['harmonic']["harmonic_pairs"]
    alpha = config['harmonic']["alpha"]

    # process
    # Get variables and target
    fnames = os.listdir(dir_ard)
    fnames = list(filter(lambda fname: (".img" in fname and pol in fname and ".aux.xml" not in fname) |
                                       ('.tif' in fname and pol in fname), fnames))
    fnames = _sort_fnames(fnames)
    days = _get_doy(fnames, freq)
    x = _getVariable(freq, days, 1 + num_pair * 2)
    del days

    # Harmonic regression fitting
    fnames = list(map(lambda fname: os.path.join(dir_ard, fname), fnames))

    # Read meta image
    img_tep = gdal.Open(fnames[0])
    n_rows = img_tep.RasterYSize
    n_cols = img_tep.RasterXSize
    trans = img_tep.GetGeoTransform()
    proj = img_tep.GetProjection()
    d_type = img_tep.GetRasterBand(1).DataType
    range_row = range(n_rows)
    range_col = range(n_cols)
    img_tep = None

    # Read all images
    values = [read_img(fname) for fname in fnames]
    # Define output
    lasso_coefs = np.zeros(shape=(2 + num_pair * 2, n_rows, n_cols),
                           dtype="float32")
    # Do calculation
    for row_each in range_row:
        # read a single line from all images
        values_row = np.array([value[row_each, :] for value in values])
        coefs_row = np.zeros((2 + num_pair * 2, n_cols))
        for col_each in range_col:
            y = values_row[:, col_each]
            x_new = x[~np.isnan(y)]
            y_new = y[~np.isnan(y)]
            if y_new.size != 0:
                lasso = Lasso(max_iter=10000, alpha=alpha).fit(x_new, y_new)
                coefficients = lasso.coef_
                intercept = lasso.intercept_
            else:
                coefficients = np.empty((1 + num_pair * 2,))
                coefficients[:] = np.nan
                intercept = np.nan
            coefs = np.insert(coefficients, 0, intercept)
            coefs_row[:, col_each] = coefs
        lasso_coefs[:, row_each, :] = coefs_row

    # Write out
    dst_path = os.path.join(dir_coefs, "tile{}_{}_harmonic.tif".format(tile_index, pol))
    driver = gdal.GetDriverByName("GTiff")
    out_data = driver.Create(dst_path, n_cols, n_rows, len(lasso_coefs), d_type)
    for i in range(len(lasso_coefs)):
        out_data.GetRasterBand(i + 1).WriteArray(lasso_coefs[i, :, :])
        out_data.FlushCache()
    out_data.SetGeoTransform(trans)
    out_data.FlushCache()
    out_data.SetProjection(proj)
    out_data.FlushCache()
    out_data = None


def harmonic_fitting_br(tile_index, pol, config):
    """Fit harmonic regression coefficients for a tile.

    Args:
        tile_index (str): the index of tile.
        pol (str): polarization mode, VV or VH.
        config (dict): config dictionary.
    """
    # directions
    dir_ard = config['dirs']['dir_ard']
    dir_ard = "{}_{}".format(dir_ard, tile_index)
    dir_coefs = join(config['dirs']['dst_dir'], config['dirs']['dir_coefs'])  # dir_out
    if not os.path.isdir(dir_ard):
        sys.exit("No ARD generated for tile {}".format(tile_index))
    if not os.path.isdir(dir_coefs):
        os.mkdir(dir_coefs)

    # harmonic parameters
    freq = config['harmonic']["harmonic_frequency"]
    num_pair = config['harmonic']["harmonic_pairs"]
    alpha = config['harmonic']["alpha"]

    # process
    # Get variables and target
    fnames = os.listdir(dir_ard)
    fnames = list(filter(lambda fname: ".img" in fname and pol in fname and ".aux.xml" not in fname, fnames))
    fnames = _sort_fnames(fnames)
    days = _get_doy(fnames, freq)
    x = _getVariable(freq, days, 1 + num_pair * 2)
    del days

    # Harmonic regression fitting
    fnames = list(map(lambda fname: os.path.join(dir_ard, fname), fnames))

    # Read meta image
    img_tep = gdal.Open(fnames[0])
    n_rows = img_tep.RasterYSize
    n_cols = img_tep.RasterXSize
    trans = img_tep.GetGeoTransform()
    proj = img_tep.GetProjection()
    d_type = img_tep.GetRasterBand(1).DataType
    range_row = range(n_rows)
    range_col = range(n_cols)
    img_tep = None

    # Read all images
    values = [read_img(fname) for fname in fnames]
    # Define output
    lasso_coefs = np.memmap("tmp_{}.dat".format(tile_index),
                            dtype="float32",
                            mode="w+",
                            shape=(2 + num_pair * 2, n_rows, n_cols))

    def _cal_row(row_each, values_row, cols):
        # read a single line from all images
        # values_row = np.array([value[row_each, :] for value in values_all])
        # values_row = np.array([read_rows(fname, row_each, n_cols) for fname in fnames_all])
        coefs_row = np.zeros((2 + num_pair * 2, n_cols))
        for col_each in cols:  # range_col
            y = values_row[:, col_each]
            x_new = x[~np.isnan(y)]
            y_new = y[~np.isnan(y)]
            if y_new.size != 0:
                lasso = Lasso(max_iter=10000, alpha=alpha).fit(x_new, y_new)
                coefficients = lasso.coef_
                intercept = lasso.intercept_
            else:
                coefficients = np.empty((1 + num_pair * 2,))
                coefficients[:] = np.nan
                intercept = np.nan
            coefs = np.insert(coefficients, 0, intercept)
            coefs_row[:, col_each] = coefs
        lasso_coefs_reopen = np.memmap("tmp_{}.dat".format(tile_index),
                                       dtype="float32",
                                       mode="r+",
                                       shape=(2 + num_pair * 2, n_rows, n_cols))
        lasso_coefs_reopen[:, row_each, :] = coefs_row
        del lasso_coefs_reopen

    threads_number = config['parallel']['threads_number']
    if threads_number == 'default':
        threads_number = mp.cpu_count()
    else:
        threads_number = int(threads_number)
    mp.set_start_method("fork", force=True)
    pool = mp.Pool(processes=threads_number)
    pool.starmap(_cal_row, [(row_one, np.array([value[row_one, :] for value in values]), range_col)
                            for row_one in range_row])
    pool.close()
    pool.join()

    # Write out
    dst_path = os.path.join(dir_coefs, "tile{}_{}_harmonic.tif".format(tile_index, pol))
    driver = gdal.GetDriverByName("GTiff")
    out_data = driver.Create(dst_path, n_cols, n_rows, len(lasso_coefs), d_type)
    for i in range(len(lasso_coefs)):
        out_data.GetRasterBand(i + 1).WriteArray(lasso_coefs[i, :, :])
        out_data.FlushCache()
    out_data.SetGeoTransform(trans)
    out_data.FlushCache()
    out_data.SetProjection(proj)
    out_data.FlushCache()
    out_data = None
    del lasso_coefs
    os.unlink("tmp_{}.dat".format(tile_index))


def s1_harmonic_each(tile_index, config_path, logger,
                     gf_out_format='ENVI', thread_clip=1,
                     big_ram=False):
    """Fit harmonic regression coefficients for a tile.

    Args:
        tile_index (str): the index of tile.
        config_path (str): the path of config yaml.
        logger(logging.Logger): the logger object to store logs.
        gf_out_format (str); the format of output.
        Now it only supports [ENVI, GTiff].
        thread_clip(int): the thread number for clipping.
        big_ram(bool): if use parallel or not.
    """
    # Clip the list of imagery for each tile
    # with open(join(os.path.dirname(__file__), 's1_gather_tile.R'), 'r') as f:
    #     string = f.read()
    # func = STAP(string, "func")
    # func.gather_image(tile_index, config_path)
    cmd = 'Rscript {} -i {} -d {} -n {} -c {}' \
        .format(join(os.path.dirname(__file__), 's1_gather_tile_cli.R'),
                str(tile_index), os.path.dirname(__file__), thread_clip, config_path)
    _run_cmd(cmd, logger)
    logger.info("Finish clipping for tile {}".format(tile_index))

    # Apply guided filter
    config = _load_yaml(config_path)
    guided_filter_batch(str(tile_index), config, gf_out_format, logger)
    logger.info("Finish guided filter for tile {}".format(tile_index))

    # Calculate harmonic coefficients
    for pol in ['VV', 'VH']:
        if big_ram:
            harmonic_fitting_br(str(tile_index), pol, config)
        else:
            harmonic_fitting(str(tile_index), pol, config)
        logger.info("Finish guided filter for  {} of tile {}".format(pol, tile_index))

    # Remove all temporary files
    if not config['harmonic']['keep_mid']:
        dir_clip = config['dirs']['dir_clip']
        dir_clip = "{}_{}".format(dir_clip, tile_index)
        dir_ard = config['dirs']['dir_ard']
        dir_ard = "{}_{}".format(dir_ard, tile_index)
        shutil.rmtree(dir_clip)
        shutil.rmtree(dir_ard)


def s1_harmonic_batch(config_path, gf_out_format='ENVI',
                      initial=False, parallel_tile=False,
                      big_ram=False, thread_clip=2):
    """Batch fit harmonic regression coefficients.

    Args:
        config_path (str): the path of config yaml.
        gf_out_format (str); the format of output.
        Now it only supports [ENVI, GTiff].
        initial (bool): if the script is run initially.
        parallel_tile (bool): if to do parallel over tiles.
        big_ram (bool): if has big ram to use.
        thread_clip(int): the thread number for clipping.
    """
    config = _load_yaml(config_path)
    # Set up logger
    dst_dir = config['dirs']['dst_dir']
    log_dir = join(dst_dir, 'logs')
    if not exists(log_dir):
        os.makedirs(log_dir)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    log_path = join(log_dir, 'sentinel1_harmonic_regression_{}.log'
                    .format(datetime.now().strftime("%d%m%Y_%H%M")))
    log_format = "%(asctime)s::%(levelname)s::%(name)s::%(filename)s::%(lineno)d::%(message)s"
    logging.basicConfig(filename=log_path, filemode='w',
                        level=logging.INFO, format=log_format)
    logger = logging.getLogger(__name__)

    # Start the progress
    print('Assume that you have put all imagery in the right path.')
    # Harmonic regression of sentinel-1
    sc = SentinelClient(config)

    if initial:
        with open(join(os.path.dirname(__file__), 's1_gather_tile.R'), 'r') as f:
            string = f.read()
        func = STAP(string, "func")
        func.split_catalog(config_path)

    # Determine thread number to be used
    threads_number = config['parallel']['threads_number']
    if threads_number == 'default':
        threads_number = mp.cpu_count()
    else:
        threads_number = int(threads_number)

    if parallel_tile:
        harmonic_executor = FixedThreadPoolExecutor(size=threads_number)
        # Batch process
        success_count = 0
        failure_count = 0
        for i in range(0, len(sc.footprint['features'])):
            tile_index = sc.footprint['features'][i]['properties']['tile']
            if isinstance(tile_index, float):
                tile_index = str(int(tile_index))
            else:
                tile_index = str(tile_index)
            # tile_index = i + 1
            if harmonic_executor.submit(s1_harmonic_each,
                                        tile_index, config_path,
                                        logger, gf_out_format,
                                        thread_clip) is True:
                success_count = success_count + 1
            else:
                failure_count = failure_count + 1
        # await all tile finished
        harmonic_executor.drain()
        # await thread pool to stop
        harmonic_executor.close()
        logger.info("Sentinel1_harmonic_regression: finished harmonic task; "
                    "the total tile number to be processed is {}; "
                    "the success_count is {}; the failure_count is {}"
                    .format(len(sc.footprint['features']), success_count, failure_count))
    else:
        for i in range(0, len(sc.footprint['features'])):
            tile_index = sc.footprint['features'][i]['properties']['tile']
            if isinstance(tile_index, float):
                tile_index = str(int(tile_index))
            else:
                tile_index = str(tile_index)
            # tile_index = i + 1
            s1_harmonic_each(tile_index, config_path,
                             logger, gf_out_format,
                             thread_clip, big_ram)
        logger.info("Sentinel1_harmonic_regression: finished harmonic task; "
                    "the total tile number to be processed is {}."
                    .format(len(sc.footprint['features'])))
    print('s1_harmonic_batch is done, please check {} for logs.'.format(log_path))


def s2_wasp(tile_id, config, logger=None):
    """Do cloud and shadow detection using WASP for sentinel2 imagery

    Args:
        tile_id (str): the tile id of sentinel-2 tile, e,g, T36MVB.
        logger (logging.Logger): the logger object to store logs.
        config (dict): the dictionary of parameters for fmask.
    """
    # Set paths
    processed_path = join(config['dirs']['dst_dir'],
                          config['dirs']['processed_path'])
    level3_processed_path = join(config['dirs']['dst_dir'],
                                 config['dirs']['level3_processed_path'])
    # tmp_wasp_path = join(config['dirs']['dst_dir'], 'tmp_wasp')
    tmp_wasp_path = join(os.getcwd(), 'tmp_wasp_{}'.format(tile_id))
    docker = config['wasp']['docker']

    # Check paths
    if processed_path is None:
        logger.error('Did not set path of processed path.')
        sys.exit('Did not set path of processed path.')
    if not exists(level3_processed_path):
        os.mkdir(level3_processed_path)
        logger.info('Create processed path {}.'.format(level3_processed_path))

    # Unzip
    fnames_all = os.listdir(processed_path)
    fnames_all = list(filter(lambda fname: join(processed_path, fname).endswith('zip'), fnames_all))
    zips = list(filter(lambda fname: tile_id in fname, fnames_all))
    for zip_file in zips:
        _unzip_file(zip_file, processed_path)

    # Format tile_id
    time_series = config['wasp']['time_series']
    fnames_all = os.listdir(processed_path)
    fnames_all = list(filter(lambda fname: os.path.isdir(join(processed_path, fname)), fnames_all))
    safe_path = list(filter(lambda fname: tile_id in fname, fnames_all))
    for i in range(len(time_series)):
        # # Has to be the full calendar, not flexible
        # if i == len(time_series) - 1:
        #     d1 = datetime.strptime(str(time_series[i]), "%Y-%m-%d")
        #     yr_old = re.search('[0-9]{4}', str(time_series[0])).group(0)
        #     yr = str(int(yr_old) + 1)
        #     d2_str = str(time_series[0]).replace(yr_old, yr)
        #     d2 = datetime.strptime(d2_str, "%Y-%m-%d") - timedelta(days=1)
        # else:
        #     d1 = datetime.strptime(str(time_series[i]), "%Y-%m-%d")
        #     d2 = datetime.strptime(str(time_series[i + 1]), "%Y-%m-%d")
        # The user has to define the start date and end date.
        d1 = datetime.strptime(str(time_series[i]), "%Y-%m-%d")
        d2 = datetime.strptime(str(time_series[i + 1]), "%Y-%m-%d")
        safe_path_sub = list(filter(lambda fname:
                                    d1 <= datetime.strptime(re.search("[0-9]{8}", fname).group(0), "%Y%m%d") < d2,
                                    safe_path))
        if len(safe_path_sub) > 0:
            if docker:
                # Check docker
                cmd = 'docker image ls | grep "wasp"'
                if not _run_cmd(cmd, logger):
                    logger.error('There is no docker image wasp running. Please use wasp_docker_install to install.')
                    sys.exit('There is no docker image wasp running. Please use wasp_docker_install to install.')

                # Copy file
                if not exists(tmp_wasp_path):
                    os.mkdir(tmp_wasp_path)
                for each in safe_path_sub:
                    os.mkdir(join(tmp_wasp_path, each))
                    _copytree(join(processed_path, each),
                              join(tmp_wasp_path, each))

                # Run wasp
                # get parameters
                half_time = int(math.floor((d2 - d1).days / 2))
                centered_date = d1 + timedelta(days=half_time)
                cmd = 'docker run -v {}:/mnt/input-dir:ro -v ' \
                      '{}:/mnt/output-dir:rw wasp ' \
                      '{} {}'.format(tmp_wasp_path, level3_processed_path,
                                     format(centered_date, '%Y%m%d'), half_time)
                if _run_cmd(cmd, logger):
                    if logger is None:
                        print("Finish wasp for tile for {} between {} and {}".format(tile_id, d1, d2))
                    else:
                        logger.info("Finish wasp for tile for {} between {} and {}".format(tile_id, d1, d2))
                else:
                    if logger is None:
                        print("Fail to finish wasp for tile for {} between {} and {}".format(tile_id, d1, d2))
                    else:
                        logger.error("Fail to finish wasp for tile for {} between {} and {}".format(tile_id, d1, d2))
                if exists(tmp_wasp_path):
                    shutil.rmtree(tmp_wasp_path)
            else:
                print('No local mode support yet.')
        else:
            if logger is None:
                print("There is no atmospheric corrected tile for {} between {} and {}".format(tile_id, d1, d2))
            else:
                logger.warning(
                    "There is no atmospheric corrected tile for {} between {} and {}".format(tile_id, d1, d2))
            sys.exit("There is no atmospheric corrected tile for {} between {} and {}".format(tile_id, d1, d2))

    # Delete unzipped files
    for each in safe_path:
        shutil.rmtree(join(processed_path, each))


def s2_wasp_batch(config_path):
    """Batch run WASP on sentinel-2 imagery

    Args:
        config_path (str): the path of config yaml.
    """
    config = _load_yaml(config_path)
    # Set up logger
    dst_dir = config['dirs']['dst_dir']
    processed_path = join(config['dirs']['dst_dir'],
                          config['dirs']['processed_path'])
    log_dir = join(dst_dir, 'logs')
    if not exists(log_dir):
        os.makedirs(log_dir)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    log_path = join(log_dir, 'sentinel2_wasp_{}.log'
                    .format(datetime.now().strftime("%d%m%Y_%H%M")))
    log_format = "%(asctime)s::%(levelname)s::%(name)s::%(filename)s::%(lineno)d::%(message)s"
    logging.basicConfig(filename=log_path, filemode='w',
                        level=logging.INFO, format=log_format)
    logger = logging.getLogger(__name__)

    # WASP of sentinel-2
    # Get tile ids
    fnames_all = os.listdir(processed_path)
    fnames_all = list(filter(lambda fname: os.path.isdir(join(processed_path, fname)), fnames_all))
    if len(fnames_all) == 0:
        fnames_all = os.listdir(processed_path)
        fnames_all = list(filter(lambda fname: fname.endswith('zip'), fnames_all))
    tile_ids = list(set(map(lambda fname: re.search("T[0-9]{2}[A-Z]{3}", fname).group(0), fnames_all)))

    # Determine thread number to be used
    threads_number = config['parallel']['threads_number']
    if threads_number == 'default':
        threads_number = mp.cpu_count()
    else:
        threads_number = int(threads_number)
    wasp_executor = FixedThreadPoolExecutor(size=threads_number)

    # Batch process
    success_count = 0
    failure_count = 0
    for tile_id in tile_ids:
        if wasp_executor.submit(s2_wasp, tile_id, config, logger) is True:
            success_count = success_count + 1
        else:
            failure_count = failure_count + 1
    # await all tile finished
    wasp_executor.drain()
    # await thread pool to stop
    wasp_executor.close()
    logger.info("Sentinel2_wasp: finished wasp task; "
                "the total tile number to be processed is {}; "
                "the success_count is {}; the failure_count is {}"
                .format(len(tile_ids), success_count, failure_count))
    print('s2_wasp_batch is done, please check {} for logs.'.format(log_path))
