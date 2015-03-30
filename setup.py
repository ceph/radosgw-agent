from setuptools import setup, find_packages
import sys
import re


module_file = open("radosgw_agent/__init__.py").read()
metadata = dict(
    re.findall(r"__([a-z]+)__\s*=\s*['\"]([^'\"]*)['\"]", module_file))


install_requires = []
pyversion = sys.version_info[:2]
if pyversion < (2, 7) or (3, 0) <= pyversion <= (3, 1):
    install_requires.append('argparse')

setup(
    name='radosgw-agent',
    version=metadata['version'],
    packages=find_packages(),

    author='Josh Durgin',
    author_email='jdurgin@redhat.com',
    description='Synchronize users and data between radosgw clusters',
    license='MIT',
    keywords='radosgw ceph radosgw-agent',
    url="https://github.com/ceph/radosgw-agent",

    install_requires=[
        'setuptools',
        'boto >=2.2.2,<3.0.0',
        ] + install_requires,

    test_requires=[
        'pytest >=2.1.3',
        'mock >=1.0',
        'httpretty',
        ],

    entry_points={
        'console_scripts': [
            'radosgw-agent = radosgw_agent.cli:main',
            ],
        },
    )
