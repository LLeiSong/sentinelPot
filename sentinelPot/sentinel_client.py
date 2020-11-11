"""
This is a script for sentinel class.
Author: Lei Song
Maintainer: Lei Song (lsong@clarku.edu)
"""
import os
import geojson
import datetime as dt
from os.path import join
from geojson import dump
from sentinelsat import SentinelAPI
from sentinelsat import geojson_to_wkt, read_geojson


class SentinelClient:
    """The class for sentinel.
    Attributes:
        date_start (dt.datetime): the start date for query.
        date_end (dt.datetime): the end date for query.
        footprint (str): the path of geojson for query.
        api (sentinelsat.SentinelAPI): sentinel API defined by sentinelsat.
        platformname (str): platform name for query.
            sentinel-1 or sentinel-2.
        producttype (str): product type for query.
            SLC, GRD, OCN for sentinel-1, or ... for sentinel-2.
        sensoroperationalmode: operation mode for query.
            SM, IW, EW, WV for sentinel-1, or ... for sentinel-2.
        cloudcover (list of int, optional): the range of cloud over for query.
        directory_path (str): the path for downloading.
        footprint_list (str): the path of query catalog geojson.
    """

    def __init__(self, config):
        """Example of docstring on the __init__ method.
        Args:
            config (dict): the dictionary of configs.
        """
        # configurations
        sc_config = config['sentinel']
        self.date_start = dt.datetime.strptime(str(sc_config['date_start']), "%Y-%m-%d")
        self.date_end = dt.datetime.strptime(str(sc_config['date_end']), "%Y-%m-%d")
        self.footprint = read_geojson(sc_config['geojson'])
        self.api = SentinelAPI(config['sci_hub']['user'],
                               config['sci_hub']['password'],
                               'https://scihub.copernicus.eu/dhus')
        self.platformname = sc_config['platformname']
        self.producttype = sc_config['producttype']
        self.sensoroperationalmode = sc_config['sensoroperationalmode']
        if sc_config['clouds'] is None:
            self.cloudcover = 100
        else:
            self.cloudcover = int(sc_config['clouds'])
        self.directory_path = join(config['dirs']['dst_dir'],
                                   config['dirs']['download_path'])
        self.footprint_list = sc_config['catalog_json']

    def get_scenes(self):
        """Get sentinel scenes
        Now it only supports sentinel-1 and sentinel-2,
        but it could be expanded easily.

        Returns:
            collections.OrderedDict: an ordered list of scenes.
        """
        # Sentinel-1 query
        if self.platformname == 'S1':
            ftpt = geojson_to_wkt(self.footprint)
            all_scenes = self.api.query(ftpt,
                                        date=(self.date_start,
                                              self.date_end),
                                        platformname='Sentinel-1',
                                        producttype=self.producttype,
                                        sensoroperationalmode=self.sensoroperationalmode)
            # geojson with more than one features
            if len(self.footprint['features']) > 1:
                for i in range(1, len(self.footprint['features'])):
                    ftpt = geojson_to_wkt(self.footprint, feature_number=i)
                    scenes = self.api.query(ftpt,
                                            date=(self.date_start,
                                                  self.date_end),
                                            platformname='Sentinel-1',
                                            producttype=self.producttype,
                                            sensoroperationalmode=self.sensoroperationalmode)
                    all_scenes.update(scenes)
        # Sentinel-2 query
        elif self.platformname == 'S2':
            ftpt = geojson_to_wkt(self.footprint)
            all_scenes = self.api.query(ftpt,
                                        date=(self.date_start,
                                              self.date_end),
                                        platformname='Sentinel-2',
                                        cloudcoverpercentage=(0, self.cloudcover))
            # geojson with more than one features
            if len(self.footprint['features']) > 1:
                for i in range(1, len(self.footprint['features']) - 1):
                    ftpt = geojson_to_wkt(self.footprint, feature_number=i)
                    scenes = self.api.query(ftpt,
                                            date=(self.date_start,
                                                  self.date_end),
                                            platformname='Sentinel-2',
                                            cloudcoverpercentage=(0, self.cloudcover))
                    all_scenes.update(scenes)
        # Need to expand to include more
        else:
            exit("Not supported producttype in this class.")
        return all_scenes

    def make_footprints(self):
        """Make a footprint geojson catalog for imagery
        if the footprint has more than one features.
        It supports only sentinel-1 and sentinel-2 for now,
        but it could be easily expanded to include more.

        Returns:
            geojson.feature.FeatureCollection: the feature collection of footprint.
        """
        ftpt = geojson_to_wkt(self.footprint)
        try:
            tile_index = self.footprint[0]['properties']['tile']
        except KeyError as e:
            exit("Error in reading geojson: {}".format(e))
        if self.platformname == 'S1':
            scenes = self.api.query(ftpt,
                                    date=(self.date_start,
                                          self.date_end),
                                    platformname='Sentinel-1',
                                    producttype=self.producttype,
                                    sensoroperationalmode=self.sensoroperationalmode)
        elif self.platformname == 'S2':
            scenes = self.api.query(ftpt,
                                    date=(self.date_start,
                                          self.date_end),
                                    platformname='Sentinel-2',
                                    cloudcoverpercentage=(0, self.cloudcover))
        # Place to expand to include more
        else:
            exit("Not supported producttype in this class.")
        feature_collection = self.api.to_geojson(scenes)
        for n in range(0, len(feature_collection['features'])):
            feature_collection['features'][n]['properties']['tile_index'] = tile_index
        feature_list = list(feature_collection['features'])

        # Geojson with more than one features
        if len(self.footprint['features']) > 1:
            for i in range(1, len(self.footprint['features'])):
                ftpt = geojson_to_wkt(self.footprint, feature_number=i)
                try:
                    tile_index = self.footprint[i]['properties']['tile']
                except KeyError as e:
                    exit("Error in reading geojson: {}".format(e))
                if self.platformname == 'S1':
                    scenes = self.api.query(ftpt,
                                            date=(self.date_start,
                                                  self.date_end),
                                            platformname='Sentinel-1',
                                            producttype=self.producttype,
                                            sensoroperationalmode=self.sensoroperationalmode)
                elif self.platformname == 'S2':
                    scenes = self.api.query(ftpt,
                                            date=(self.date_start,
                                                  self.date_end),
                                            platformname='Sentinel-2',
                                            cloudcoverpercentage=(0, self.cloudcover))
                # Need to expand to include more
                else:
                    exit("Not supported producttype in this class.")
                feature_collection = self.api.to_geojson(scenes)
                for n in range(0, len(feature_collection['features'])):
                    feature_collection['features'][n]['properties']['tile_index'] = tile_index
                feature_list.extend(list(feature_collection['features']))
        feature_collections = geojson.FeatureCollection(feature_list)

        # Save out the feature collection
        try:
            with open(self.footprint_list, 'w') as f:
                dump(feature_collections, f)
        except OSError:
            print('Failed to save footprint.')

        return feature_collections

    def download_one_scihub(self, scene_id, download=True, trigger=True):
        """Download one imagery based on scene id.

        Args:
            scene_id (str): the scene_id to download
            trigger (bool): option if trigger off-line image or not.
            download (bool): option if download online image or not.

        Returns:
            True if online, otherwise api.download message.
        """
        product_info = self.api.get_product_odata(scene_id)
        if product_info['Online']:
            if download:
                return self.api.download(scene_id, directory_path=self.directory_path)
            else:
                return True
        else:
            if trigger:
                return self.api.download(scene_id, directory_path=self.directory_path)
            else:
                return False

    def download_all_scihub(self, scenes):
        """Download a list of imagery

        Args:
            scenes (collections.OrderedDict): the Ordered dictionary of scenes.

        Returns:
            api.download message.
        """
        return self.api.download_all(scenes, directory_path=self.directory_path)

    @staticmethod
    def get_scene_ids(scenes):
        """Static function to get the scene ids

        Args:
            scenes (collections.OrderedDict): the Ordered dictionary of scenes.

        Returns:
            list: a list of scene ids.
        """
        return list(scenes.keys())

    @staticmethod
    def get_scene_titles(scenes):
        """Static function to get the scene identifiers

        Args:
            scenes (collections.OrderedDict): the Ordered dictionary of scenes.

        Returns:
            list: scene_titles, a list of scene titles.
        """
        scene_titles = []
        for key, item in scenes.items():
            scene_titles.append(item['title'])
        return scene_titles

    def get_finished_titles(self):
        """Get finished titles.

        Returns:
            list: a list of titles of downloaded files.
        """

        fnames = os.listdir(self.directory_path)
        fnames = list(filter(lambda fn: ".zip" in fn, fnames))
        fnames = list(map(lambda fn: fn.replace('.zip', ''), fnames))
        return fnames
