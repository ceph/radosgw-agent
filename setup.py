#!/usr/bin/python
from setuptools import setup, find_packages
import sys


install_requires = []
pyversion = sys.version_info[:2]
if pyversion < (2, 7) or (3, 0) <= pyversion <= (3, 1):
    install_requires.append('argparse')

setup(
    name='radosgw-agent',
    version='1.0',
    packages=find_packages(),

    author='Josh Durgin',
    author_email='josh.durgin@inktank.com',
    description='Synchronize users and data between radosgw clusters',
    license='MIT',
    keywords='radosgw ceph radosgw-agent',
    url="https://github.com/ceph/radosgw-agent",

    install_requires=[
        'setuptools',
        'boto ==2.2.2',
        'requests',
        ] + install_requires,

    entry_points={
        'console_scripts': [
            'radosgw-agent = radosgw_agent.cli:main',
            ],
        },
    )
