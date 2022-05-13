#!/usr/bin/env python

from setuptools import setup

setup(name='tap-jsonl',
      version='0.0.1',
      description='Singer.io tap for extracting data from a jsonl file',
      author='Hotglue',
      url='http://hotglue.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_jsonl'],
      install_requires=[
          'singer-python==5.7.0',
          'jsonlines==3.0.0'
      ],
      entry_points='''
          [console_scripts]
          tap-jsonl=tap_jsonl:main
      ''',
      packages=['tap_jsonl'],
      package_data = {
          'tap_jsonl/schemas': [
          ],
      },
      include_package_data=True,
)

