from setuptools import setup, find_packages

setup(
  name='pyspark_sandbox',
  version='1.0.0',
  author="Unknown User",
  url='https://github.com/',
  author_email='unknown@gmail.com',
  description='pyspark sandbox',
  packages=find_packages(include=['src']),
  entry_points={
    'group_1': 'run=src.__main__:main'
  },
  install_requires=[
    'setuptools'
  ]
)