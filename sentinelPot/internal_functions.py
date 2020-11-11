"""
This is a script for main common internal functions.
Author: Lei Song
Maintainer: Lei Song (lsong@clarku.edu)
"""
import os
import yaml
import shutil
from os.path import join
from zipfile import ZipFile
import subprocess


def _load_yaml(config_path, logger=None):
    """Load config yaml file

    Args:
        config_path (str): the path of config yaml.
        logger (logging.Logger): the logger object to store logs.

    Returns:
        dict: config, a dictionary of configs.
    """
    try:
        with open(config_path, 'r') as yaml_file:
            config = yaml.safe_load(yaml_file)
            return config
    except OSError:
        if logger is None:
            print("Cannot open", config_path)
        else:
            logger.error("Cannot open", config_path)
        raise


def _unzip_file(fname, download_path, keep=True):
    """Unzip the file

    Args:
        download_path (str): the path to download files.
        fname (str): the name of file.
        keep (bool): the option to keep the zip raw file.
    """
    ZipFile(join(download_path, fname)).extractall(download_path)
    if not keep:
        os.remove(join(download_path, fname))


def _copytree(src, dst, symlinks=False, ignore=None):
    """Unzip the file

    Args:
        src (str): the source path.
        dst (str): the destination path.
        symlinks (bool): the option to use symlinks or not.
        ignore (str): the ignore files.
    """
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)


def _get_files_recursive(dir_name):
    """Get the files in a folder recursively

    Args:
        dir_name (str): the name of directory.

    Returns:
        list: all_files, a list of all files in the folder.
    """
    # create a list of file and sub directories
    # names in the given directory
    list_dir = os.listdir(dir_name)
    all_files = list()
    # Iterate over all the entries
    for entry in list_dir:
        # Create full path
        full_path = os.path.join(dir_name, entry)
        # If entry is a directory then get the list of files in this directory
        if os.path.isdir(full_path):
            all_files = all_files + _get_files_recursive(full_path)
        else:
            all_files.append(full_path)
    return all_files


def _run_cmd(cmd, logger=None):
    """Using os to run a command line

    Args:
        cmd (str): a command line string.
        logger (logging.Logger): the logger object to store logs.

    Returns:
        bool: True if success, otherwise False
    """
    run_it = subprocess.Popen(cmd, shell=True).wait()
    if run_it == 0:
        return True
    else:
        if logger is None:
            print("Failed to run cmd {}.".format(cmd))
        else:
            logger.error("Failed to run cmd {}.".format(cmd))
        return False


def _delete_files(fnames, logger=None):
    """Delete the file given a specific path

    Args:
        fnames (str): full path of file to delete.
        logger (logging.Logger): the logger object to store logs.
    """
    for fname in fnames:
        try:
            os.remove(fname)
        except OSError as e:
            if logger is None:
                print("Removing {} fails: {} ".format(fname, e))
            else:
                logger.error("Removing {} fails: {} ".format(fname, e))


def _divide_chunks(l, n):
    """Split a list with fixed length

    Args:
        l (list): the list to be split.
        n (int): the length of sub-lists.
    """
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]
