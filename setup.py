"""
Setup script.

Use this script to install the ZeroMQ reader/writer incremental modules for the retico framework.
Usage:
    $ python3 setup.py install
The run the simulation:
    $ retico [-h]
"""

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

config = {
    "description": "The ZeroMQ reader/writer incremental modules for the retico framework",
    "author": "Casey Kennington",
    "url": "??",
    "download_url": "??",
    "author_email": "caseykennington@boisestate.edu",
    "version": "0.1",
    "install_requires": ["retico-core~=0.2.0"],
    "packages": find_packages(),
    "name": "retico-zmq",
}

setup(**config)