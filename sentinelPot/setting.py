"""
This is a chunk which includes functions to set up environment.
Author: Lei Song
Maintainer: Lei Song (lsong@clarku.edu)
"""
import os
import re
import gdown
from os.path import join
from sys import platform, exit
from shutil import copyfile
from .internal_functions import _run_cmd


def generate_config_file(dst_dir='.', logger=None):
    """Download file

    Args:
        dst_dir (str): the destination dir.
        logger (logging.Logger): logger object to store logs..
    """
    c_dir = os.path.dirname(__file__)
    src_path = '{}/files/config_main_template.yaml'.format(c_dir)
    dst_path = '{}/files/config_main.yaml'.format(dst_dir)
    copyfile(src_path, dst_path)
    if logger is None:
        print("Generate config yaml file: {}", dst_path)
        print("Please add values based on your own needs.")
    else:
        logger.info("Generate config yaml file: {}", dst_path)
        logger.info("Please add values based on your own needs.")


def sen2cor_install(dst_dir=None, logger=None):
    """Install sen2cor
    Just test for Linux and macOS.

    Args:
        dst_dir (str): the destination dir.
        logger (logging.Logger): logger object to store logs.
    Returns:
        str or bool: sen2cor path if success, otherwise False.
    """
    if dst_dir is None:
        dst_dir = os.getenv("HOME")

    # Detect operation system
    if platform.startswith('linux'):
        http_address = r'http://step.esa.int/thirdparties/sen2cor/2.8.0/Sen2Cor-02.08.00-Linux64.run'
        app_name = re.search('Sen2Cor-(.+).run', http_address).group(0)
    elif platform.startswith("darwin"):
        http_address = r'http://step.esa.int/thirdparties/sen2cor/2.8.0/Sen2Cor-02.08.00-Darwin64.run'
        app_name = re.search('Sen2Cor-(.+).run', http_address).group(0)
    elif platform.startswith('windows'):
        if logger is None:
            print("Windows machine: please install sen2cor manually.")
        else:
            logger.warning("Windows machine: please install sen2cor manually.")
        exit("Windows machine: please install sen2cor manually.")

    # Download
    fname = join(dst_dir, app_name)
    app_dl = "curl -o {} {}".format(fname, http_address)
    if _run_cmd(app_dl, logger):
        # Install
        target_ins = join(dst_dir, "sen2cor240")
        app_ins = "chmod +x {}; sh {} --target {}".format(fname, fname, target_ins)
        if _run_cmd(app_ins, logger):
            os.remove(fname)
            exe_path = join(target_ins, 'bin/L2A_Process')
            if logger is None:
                print("Install sen2cor successfully to {}.".format(exe_path))
            else:
                logger.info("Install sen2cor successfully to {}.".format(exe_path))
            return exe_path
        else:
            os.remove(fname)
            if logger is None:
                print("Fail to install sen2cor.")
            else:
                logger.error("Fail to install sen2cor.")
            return False
    else:
        if logger is None:
            print("Fail to download sen2cor.")
        else:
            logger.error("Fail to download sen2cor.")
        return False


def fmask_docker_install(logger=None):
    """Install sen2cor
    Just test for Linux and macOS.

    Args:
        logger (logging.Logger): logger object to store logs.
    """
    # Grab fmaskilicious and Fmask4.2 installer
    c_dir = os.path.dirname(__file__)
    path_fmask = 'https://drive.google.com/uc?id=1tZmuBbKz_yca56tIWbRHRvicPY3DJ-wy'
    grab_fmaskilicious = "git clone git@github.com:LLeiSong/" \
                         "fmaskilicious.git {}/fmaskilicious".format(c_dir)
    if _run_cmd(grab_fmaskilicious, logger):
        if logger is None:
            print("Grab fmaskilicious.")
        else:
            logger.info("Grab fmaskilicious.")

        # Download Fmask4.2 installer
        gdown.download(path_fmask, join(c_dir, 'fmaskilicious/Fmask_4_2_Linux.install'), quiet=False)

        # Install docker
        install_cmd = 'cd {}/fmaskilicious; docker build -t fmask .'.format(c_dir)
        if _run_cmd(install_cmd, logger):
            if logger is None:
                print("Install fmask docker image.")
                print('How to use: docker run -v /path/to/safe:/mnt/input-dir:ro'
                      ' -v /path/to/output:/mnt/output-dir:rw -it fmask '
                      'S2A_MSIL1C_20180624T103021_N0206_R108_T33UUB_20180624T160117 *T33UUB* '
                      '[cloud_buffer_distance] [shadow_buffer_distance] '
                      '[snow_buffer_distance] [cloud_prob_threshold]')
            else:
                logger.info("Install fmask docker image.")
        else:
            if logger is None:
                print("Fail to install fmask docker image.")
            else:
                logger.error("Fail to install fmask docker image.")
    else:
        if logger is None:
            print("Fail to grab fmaskilicious.")
        else:
            logger.error("Fail to grab fmaskilicious.")


def wasp_docker_install(path_wasp=None, logger=None):
    """Install sen2cor
    Just test for Linux and macOS.

    Args:
        path_wasp (str): the full path of wasp.
        It needs token to download, so user should download it manually.
        logger (logging.Logger): logger object to store logs.
    """
    # Check wasp
    if path_wasp is None:
        dl_page = 'https://logiciels.cnes.fr/en/license/128/515'
        print('Did not set wasp path.')
        print('Please go here to download it: ').format(dl_page)
        exit('Did not set wasp path.')
    else:
        if not os.path.exists(path_wasp):
            print('No wasp detected under {}.'.format(path_wasp))
            exit('No wasp detected under {}.'.format(path_wasp))

    # Grab waspire
    c_dir = os.path.dirname(__file__)
    grab_waspire = "git clone git@github.com:LLeiSong/waspire.git " \
                   "{}/waspire".format(c_dir)
    if _run_cmd(grab_waspire, logger):
        if logger is None:
            print("Grab waspire.")
        else:
            logger.info("Grab waspire.")

        # Copy wasp
        copyfile(path_wasp, join(c_dir, 'waspire/{}'.format(os.path.basename(path_wasp))))

        # Install docker
        install_cmd = 'cd {}/waspire; docker build -t wasp .'.format(c_dir)
        if _run_cmd(install_cmd, logger):
            if logger is None:
                print("Install wasp docker image.")
                print('How to use: docker run '
                      '-v /path/to/imagery:/mnt/input-dir:ro '
                      '-v /path/for/synthesis:/mnt/output-dir:rw '
                      '-it wasp date synthalf')
            else:
                logger.info("Install wasp docker image.")
        else:
            if logger is None:
                print("Fail to install wasp docker image.")
            else:
                logger.error("Fail to install wasp docker image.")
    else:
        if logger is None:
            print("Fail to grab waspire.")
        else:
            logger.error("Fail to grab waspire.")
