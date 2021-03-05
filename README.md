# sentinelPot
[![Project Status: Active – The project has reached a stable, usable state and is being actively developed.](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)

This is a python package to assemble necessary preprocessing steps for sentinel-1&amp;2 imagery.

## Introduction

This package is just a package to wrap necessary steps to preprocess sentinel-1&2 images. 
Full credit should be given to the authors of these preprocessing methods.

### Sentinel-1

Level-2 process (Use command line graph processing framework in SNAP software):

1. Apply orbit file.
2. Thermal noise removal.
3. Border noise removal.
4. Calibration.
5. Speckle filtering.
6. Range doppler terrain correction.
7. Conversion to dB.

Level-3 process:

1. Apply guided filter to remove speckle noises further.
2. Fit harmonic regression coefficients using Lasso algorithm.
    
### Sentinel-2

Level-2 process:

Basically level-2 means atmospheric correction and cloud/shadow detection. We include three basic methods: 
sen2cor on SNAP software for atmospheric correction, Fmask for cloud/shadow detection, and MAJA installed on peps server for both. 
You could find more details here: [sen2cor](https://step.esa.int/main/snap-supported-plugins/sen2cor/), [Fmask](https://github.com/GERSL/Fmask) and [peps MAJA](https://github.com/olivierhagolle/maja_peps).
Here, we combined sen2cor and Fmask together as a regular way for level-2 process, and a MAJA way as a second method. 

Level-3 process:

Run docker to process WASP to get seasonal syntheses of sentinel-2 imagery. The details are here: [WASP](https://github.com/LLeiSong/waspire).

## Config yaml setting

The template of the yaml is as follows:

```
# Main config for the repo
dirs:
  dst_dir:
  # The prefix of dir_clip and dir_ard
  dir_clip: temp
  dir_ard: temp_ard
  # The folder for harmonic coefficients
  dir_coefs: sentinel1_hr_coefs
  # The folder for script logs
  log_dir: logs
  # The folder for peps MAJA log
  maja_log: maja_logs
  # The folder for download
  download_path: sentinel2_level1
  # The folder for level-2 processed images
  # Preprocess of S1, and atmospheric correction of S2 (fmask or maja)
  processed_path: sentinel2_level2
  # The folder for level-3
  # Harmonic regression of S1, WASP of S2
  level3_processed_path: sentinel2_level3
parallel:
  # the number of threads for parallel
  threads_number: default
# User set for peps
peps:
  user:
  password:
# User set for scihub
sci_hub:
  user:
  password:
# Sentinel query related variables
sentinel:
  # The path of geojson of tiles to query
  # there must be a column named tile to store the tile id
  geojson:
  # town name (pick one which is not too frequent to avoid confusions)
  location:
  # [lat, lon]
  point:
  # [latmin, latmax, lonmin, lonmax]
  bbox:
  # Sentinel-2 tile number
  tile:
  # the date interval for downloading, fmt('2017-10-01')
  date_start:
  date_end:
  # platform of sentinel, 'S1', 'S2', 'S2ST', 'S3'
  platformname: Sentinel-1
  # the following two for sentinel-1 only
  producttype: GRD
  sensoroperationalmode: IW
  orbit:
  # S1A,S1B,S2A,S2B,S3A,S3B
  satellite:
  # Maximum cloud coverage for sentinel-2
  clouds:
  # Path for search catalog json
  # Full path in order to run this part separately
  catalog_json: /Volumes/wildebeest/gcam/catalogs/s1_footprints_gcam_mu.geojson
  search_table: /Volumes/wildebeest/gcam/catalogs/s1_search_table_gcam_mu.csv
  # If download the imagery and download path
  download: True
  download_path:
  # Extract zipfile or not
  extract: False
  # Work on windows machine or not
  windows: False
gpt:
  # the path of SNAP GPT
  gpt_path: /Applications/snap/bin/gpt
  # the path of xml to be used in SNAP GPT
  xml_path: files/S1_GRD_preprocessing.xml
# For Sentinel-1 harmonic regression
harmonic:
  gcs_rec: 0.000025
  # the flag of reading local files or not
  if_local: false
  # only used if if_local is true
  local_path: data/s1_preprocessed
  # the polarizations of sentinel-1 GRD
  polarizations: ["VH", "VV"]
  # Filter parameters
  kernel: 3
  iteration: 3
  eps: 3
  # Harmonics parameters
  harmonic_frequency: 365
  harmonic_pairs: 2
  # Keep the moderate results
  keep_mid: False
# For sentinel-2 Fmask cloud detection
sen2cor:
  # the path of sen2cor
  sen2cor_path:
fmask:
  # use docker to run Fmask or not
  # some machines could install Fmask directly,
  # some have to use docker to run it.
  docker: true
  # If you don't use docker, set the following two paths correctly
  mc_root: /usr/local/MATLAB/MATLAB_Runtime/v96
  fmask_path: /usr/GERS/Fmask_4_2/application/run_Fmask_4_2.sh
  # some parameters for fmask
  cloudprobthreshold: 15
  cloudbufferdistance: 6
  shadowbufferdistance: 6
wasp:
  docker: True
  # Time interval to run WASP
  time_series: ['2019-03-01', '2019-05-31', '2019-10-30']
```
This file will be installed with the package. User can run `generate_config_file(dst_dir)` to make a copy of this file to any reachable directory.
It could be conveniently to combine with other user's setting as long as the needed parameters are be kept.

## Sentinel-1 preprocess

Make sure you install SNAP software beforehand. 

### Batch level-2 processing

The whole batch preprocessing depends on the tile geojson. 
As long as a good geojson is given and other settings are correct, the whole process can be run automatically.
For example:
```
from sentinelPot import s1_preprocess
config_path = './config.yaml'
s1_preprocess(config_path, query=False)
```
As we can see there is an argument `query`, which means if you want to query the raw image or not.
The query can also been run independently, like:
```
from sentinelPot import peps_downloader, ParserConfig
config_path = './config.yaml'
options = ParserConfig(config_path)
peps_downloader(options)
```

### One-time level-2 processing

You could also just run process for one file using function `s1_gpt_process(fname, gpt_path, download_path, processed_path, xml_path)`

### Batch level-3 processing

Set the proper parameters in yaml, and then:
```
from sentinelPot import s1_harmonic_batch
config_path = './config.yaml'
s1_harmonic_batch(config_path, gf_out_format='GTiff', 
    initial=False, parallel_tile=True, thread_clip=5)
```

### One-time level-3 preprocessing

Here is the function to run one-time processing:
```
s1_harmonic_each(tile_index, config_path, 
    logger, gf_out_format='ENVI', thread_clip=1, big_ram=False)
```
You have to set the logger to use it.

Here are a few steps included which you also can run independently:

1. `guided_filter(src_path, ksize, eps, dst_dir, out_format='ENVI')` to run guided filter for any image. `ksize, eps` can be found in config yaml about how to set them.
2. `guided_filter_batch(tile_index, config, out_format='ENVI', logger=None)` to batch run guided filter for a bunch of images based on tile (sentinel only).
3. `harmonic_fitting(tile_index, pol, config)` to fit harmonic regression for a defined tile (not sentinel-1 tile, but the one you defined on tile geojson).
4. `harmonic_fitting_br(tile_index, pol, config)` to fit harmonic regression for a bid defined tile.

`config` above can be get by:
```
with open("./config.yaml", 'r') as yaml_file:
       config = yaml.safe_load(yaml_file)
```

**NOTE:** better to have R installed in the machine as well. Because we applied an R script to clip the noisy border further before fitting harmonic.

## Sentinel-2 preprocess

### Install tools

- `sen2cor_install(dst_dir)` to install sen2cor for SNAP.
- `fmask_docker_install` to install docker to use Fmask. We will keep updating the docker installer to include the most updated Fmask.
- `wasp_docker_install` to install docker to use WASP, we also will keep updating the docker installer.

### Level-2 preprocessing

#### MAJA on peps server

```
from sentinelPot import s2_maja_process, ParserConfig
config_path = './config.yaml'
options = ParserConfig(config_path)
s2_maja_process(options)
```
**WARNING:** it depends on the availability and stability of peps server. 
So sometimes we have to do some manual work. This is an example chunk to manually run the process:
```
import logging
import os
import re
import sys
import time
from datetime import datetime
from os.path import join
from sentinelPot import ParserConfig, \
    parse_config, parse_catalog, \
    peps_maja_downloader, peps_maja_process, \
    peps_downloader


def _divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


config_path = 'cfgs/config_main_s2.yaml'
options = ParserConfig(config_path)
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

# Stage images to disk and get catalog
# peps_downloader(options)
prod, download_dict, storage_dict, size_dict = parse_catalog(options, logger)
prod = list(set(download_dict.keys()))
tiles_dup = list(map(lambda x: re.search("T[0-9]{2}[A-Z]{3}", x).group(0), prod))
tiles = list(set(map(lambda x: re.search("T[0-9]{2}[A-Z]{3}", x).group(0), prod)))
tiles = list(_divide_chunks(tiles, 10))

# Request maja
# Create path for logs
if not os.path.isdir(join(options.dst_dir, options.maja_log)):
    os.mkdir(join(options.dst_dir, options.maja_log))

if options.start_date is not None:
    start_date = options.start_date
    if options.end_date is not None:
        end_date = options.end_date
    else:
        end_date = datetime.date.today().isoformat()
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
```
Here, we used function `peps_maja_process` to run MAJA on peps server for one tile, 
and `peps_maja_downloader` to download the processed images.

#### Sen2cor and Fmask

`s2_fmask(tile_name, config, logger)` to run, need to set the logger.
`s2_atmospheric_correction(fname, processed_path, sen2cor_path)` to run sen2cor.
`s2_preprocess_each(tile_name, config, logger)` to run them together as a whole process.

#### Level-2 batch preprocessing

`s2_preprocess(config_path, option='regular', query=False, source='peps')` to batch run the whole process.
option "regular" means the combination of sen2cor and Fmask, and option "maja" means the MAJA method. 
We also could query the images on the fly before the preprocessing if not yet.

### Level-3 preprocessing

We just include WASP method. Here is an example of how to use it in batch:
```
from sentinelPot import s2_wasp_batch
from zipfile import ZipFile
import yaml
import os
from os.path import join

# Read yaml
config_path = 'cfgs/config_main_s2.yaml'
with open(config_path, 'r') as yaml_file:
    config = yaml.safe_load(yaml_file)

# Unzip files
download_path = join(config['dirs']['dst_dir'], config['dirs']['processed_path'])
fnames = os.listdir(download_path)
fnames = list(filter(lambda fn: '.zip' in fn, fnames))
for fname in fnames:
    ZipFile(join(download_path, fname)).extractall(download_path)

# Do WASP
# Make sure run docker before
config_path = 'cfgs/config_main_s2.yaml'
s2_wasp_batch(config_path)
```
We also could run function `s2_wasp(tile_id, config)` to process one tile. 
Still, `config` can be got by:
```
with open("./config.yaml", 'r') as yaml_file:
       config = yaml.safe_load(yaml_file)
```

## Contributors

[Lei Song](https://github.com/LLeiSong) (lsong@clarku.edu)

[Boka Luo](https://github.com/BkLuo) (bluo@clarku.edu)

We warmly welcome others to make pull request to be a contributor.

## Reference work

Many thanks to these reference work that we build upon and give full credit to them for the corresponding parts:

[Sentinel-2 GRD xml](https://github.com/ffilipponi/Sentinel-1_GRD_preprocessing)

[WASP](https://github.com/CNES/WASP)

[fmaskilicious](https://github.com/DHI-GRAS/fmaskilicious)

[maja_peps](https://github.com/olivierhagolle/maja_peps)

[peps_download](https://github.com/olivierhagolle/peps_download)

[Threading with fixed pool tool](https://gist.github.com/tliron/81dd915166b0bfc64be08b4f8e22c835)

## Acknowledgement

This package is part of project ["Combining Spatially-explicit Simulation of Animal Movement and Earth Observation to Reconcile Agriculture and Wildlife Conservation"](https://github.com/users/LLeiSong/projects/2).
This project is funded by NASA FINESST program (award number: 80NSSC20K1640).
