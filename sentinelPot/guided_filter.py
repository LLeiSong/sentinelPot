"""
This is a chunk of scripts to do guided filter for an image.
The raw code is based on their work: http://kaiminghe.com/eccv10/.
Author: Boka Luo and Lei Song
Maintainer: Lei Song (lsong@clarku.edu)
"""
import os
import sys
import cv2
try:
    import gdal
except ImportError:
    from osgeo import gdal
import numpy as np
from os.path import basename


def _boxfilter(src, ksize):
    """Apply box filter on the input image in numpy.ndarray format.

    Args:
        src (numpy.ndarray): input image in numpy.ndarray format.
        ksize (int): kernel size.

    Returns:
        numpy.ndarray: the processed image in numpy.ndarray format.
    """
    bf = cv2.boxFilter(src, -1, (ksize, ksize))
    return bf


def _guidedfilter(fsrc, src, ksize, eps):
    """Apply guided filter on the input image in numpy.ndarray of numpy.float32 format.

    Args:
        fsrc (numpy.ndarray of numpy.float32): guide image in numpy.ndarray format.
        src (numpy.ndarray of numpy.float32): image in numpy.ndarray format to be filtered.
        ksize (int): kernel size.
        eps (int): regularization parameter.

    Returns:
        numpy.ndarray of numpy.float32: the processed image in numpy.ndarray format.
    """
    mean_guided = _boxfilter(fsrc, ksize)
    mean_img = _boxfilter(src, ksize)
    mean_gi = _boxfilter(fsrc * src, ksize)
    mean_gg = _boxfilter(fsrc * fsrc, ksize)

    # covariance of (I,P) in each local patch
    cov_gi = mean_gi - mean_guided * mean_img
    var_g = mean_gg - mean_guided * mean_guided

    # equation 5 in the paper
    # Add a 0.0001 in case zero happens
    a = cov_gi / (var_g + np.float32(eps + 0.0001))
    # equation 6 in the paper
    b = mean_img - a * mean_guided

    mean_a = _boxfilter(a, ksize)
    mean_b = _boxfilter(b, ksize)

    # equation 8 in the paper
    q = mean_a * fsrc + mean_b
    return q


def guided_filter(src_path, ksize, eps, dst_dir, out_format='ENVI'):
    """Apply guided filter to a single imagery.
    The result will be saved with ENVI format.

    Args:
        src_path (str): the source path of the imagery.
        ksize (int): kernel size.
        eps (int): regularization parameter.
        dst_dir (str): the destination path of the processed imagery.
        out_format (str); the format of output. Now it only supports [ENVI, GTiff].
    """
    img = gdal.Open(src_path)
    width = img.RasterXSize
    height = img.RasterYSize
    n_band = img.RasterCount
    if out_format == 'ENVI':
        driver = gdal.GetDriverByName(out_format)
        dst_path = os.path.join(dst_dir, "{}".format(basename(src_path)))
        out_data = driver.Create(dst_path, width, height, n_band, gdal.GDT_Float32,
                                 options=["INTERLEAVE=BIP"])
    elif out_format == 'GTiff':
        driver = gdal.GetDriverByName(out_format)
        dst_path = os.path.join(dst_dir, "{}".format(basename(src_path).replace('img', 'tif')))
        out_data = driver.Create(dst_path, width, height, n_band, gdal.GDT_Float32)
    else:
        sys.exit('Not support out format. Must be [ENVI, GTiff].')
    for i in range(n_band):
        if img.GetRasterBand(i + 1).GetNoDataValue() is not None:
            novalue = np.float32(img.GetRasterBand(i + 1).GetNoDataValue())
            band_raw = np.float32(img.GetRasterBand(i + 1).ReadAsArray())
            if np.isnan(img.GetRasterBand(i + 1).GetNoDataValue()):
                band = np.where(np.isnan(band_raw), -9999, band_raw)
                band_filter = _guidedfilter(band, band, ksize, eps)
                out = np.where(np.isnan(band_raw), band_raw, band_filter)
            else:
                band = np.where(band_raw == novalue, -9999, band_raw)
                band_filter = _guidedfilter(band, band, ksize, eps)
                out = np.where(band_raw == novalue, band_raw, band_filter)
            outband = out_data.GetRasterBand(i + 1)
            # If need to set No Data Value
            outband.SetNoDataValue(img.GetRasterBand(i + 1).GetNoDataValue())
        else:
            band = np.float32(img.GetRasterBand(i + 1).ReadAsArray())
            out = _guidedfilter(band, band, ksize, eps)
            outband = out_data.GetRasterBand(i + 1)
        outband.WriteArray(out)
        out_data.FlushCache()
    out_data.SetGeoTransform(img.GetGeoTransform())
    out_data.FlushCache()
    out_data.SetProjection(img.GetProjection())
    out_data.FlushCache()
    # Close opens
    out_data = None
    img = None
    band_raw = None

    del img, band_raw, band, out_data, novalue, outband, band_filter
