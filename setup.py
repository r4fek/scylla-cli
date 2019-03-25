#!/usr/bin/env python
from setuptools import find_packages, setup

import scli as meta


with open('README.md', 'r') as f:
    README = f.read()


setup(
    name='scylla-cli',
    version='.'.join(map(str, meta.__version__)),
    description='Python script for managing and repairing Scylla Cluster',
    long_description=README,
    long_description_content_type='text/markdown',
    keywords='scylla cluster repair cli manager database nosql cassandra',
    author=meta.__author__,
    author_email=meta.__contact__,
    license='MIT',
    license_file='LICENSE',
    url=meta.__homepage__,
    packages=find_packages(exclude=('tests',)),
    include_package_data=True,
    install_requires=[
        'furl>=2.0,<2.1',
        'click==7.0',
        'click-log==0.3.2',
        'prettytable',
        'progress==1.5',
        'requests<2.22',
        'sshtunnel==0.1.4',
    ],
    entry_points={
        'console_scripts': [
            'scli=scli.main:cli',
        ]
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Database',
        'Topic :: Internet',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
