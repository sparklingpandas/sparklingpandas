from setuptools import setup
import os

VERSION = '0.0.4'
JAR_FILE = 'sparklingpandas_2.10-' + VERSION + '-SNAPSHOT.jar'
JAR_FILE_PATH = os.path.join('current-release', JAR_FILE)

setup(
    name='sparklingpandas',
    version=VERSION,
    author='Holden Karau, Juliet Hougland',
    author_email='holden@pigscanfly.ca, juliet@cloudera.com',
    packages=['sparklingpandas', 'sparklingpandas.test'],
    data_files=[
        ('bin', ['sparklingpandashell']),
        ('current-release', [JAR_FILE_PATH])],
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
