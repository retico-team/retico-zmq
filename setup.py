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

import retico_zmq

config = {
    "description": "The ZeroMQ reader/writer incremental modules for the retico framework",
    "author": "Casey Kennington",
    "url": "https://github.com/retico-team/retico-zmq",
    "download_url": "https://github.com/retico-team/retico-zmq",
    "author_email": "caseykennington@boisestate.edu",
    "version": retico_zmq.__version__,
    "install_requires": ["retico-core~=0.2.0", "zmq"],
    "packages": find_packages(),
    "name": "retico-zmq",
}

setup(**config)
