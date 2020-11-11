import re
import setuptools


with open("sentinelPot/__init__.py", encoding="utf-8") as f:
    version = re.search(r"__version__\s*=\s*'(\S+)'", f.read()).group(1)

setuptools.setup(
    name="sentinelPot",
    version=version,
    url="https://github.com/LLeiSong/sentinelPot",
    author="Lei Song",
    author_email="lsong@clarku.edu",
    description="Preprocess sentinel-1 and sentinel-2 imagery",
    long_description=open('README.md').read(),
    packages=setuptools.find_packages(),
    keywords="copernicus, sentinel, esa, satellite, process",
    install_requires=open("requirements.txt").read().splitlines(),
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
    ],
    include_package_data=True,
    package_data={'': ['files/*', 's1_gather_tile.R', 's1_gather_tile_cli.R']},
)
