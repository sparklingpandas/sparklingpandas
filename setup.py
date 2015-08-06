from setuptools import setup, find_packages
import os

VERSION = '0.0.4'
JAR_FILE = 'sparklingpandas-assembly-' + VERSION + '-SNAPSHOT.jar'

setup(
    name='sparklingpandas',
    version=VERSION,
    author='Holden Karau, Juliet Hougland',
    author_email='holden@pigscanfly.ca, juliet@cloudera.com',
    packages=find_packages(),
    include_package_data = True,
    package_data={
        'sparklingpandas.jar': ["jar/" + JAR_FILE],
        'sparklingpandas.shell': ['shell/sparklingpandasshel']},
    url='https://github.com/sparklingpandas/sparklingpandas',
    license='LICENSE.txt',
    description='Enable a Pandas like API on PySpark',
    long_description=open('README.md').read(),
    install_requires=[
        # Note: we also need PySpark 1.3 but that has to be installed manually.
        'pandas >= 0.13',
        'openpyxl>=1.6.1',
        'py4j==0.9',
        'scipy==0.16.0',
        'numpy==1.9.2'
    ],
    test_requires=[
        'nose==1.3.7',
        'coverage>3.7.0',
        'unittest2>=1.0.0'
    ],
)
