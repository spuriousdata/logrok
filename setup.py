#!/usr/bin/env python

from setuptools import setup

setup(
    name='LoGrok',
    version='0.9b',
    description='Query and Aggregate Log Data',
    author="Mike O'Malley",
    author_email='spuriousdata@gmail.com',
    url='https://github.com/spuriousdata/logrok',
    packages=['logrok'],
    install_requires=['ply>=3.4'],
    scripts=['bin/logrok'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
    ],
)
