from setuptools import setup

setup(
    name='sparklingpandas',
    version='0.0.2',
    author='Holden Karau, Juliet Hougland',
    author_email='holden@pigscanfly.ca, juliet@cloudera.com',
    packages=['sparklingpandas', 'sparklingpandas.test'],
    url='https://github.com/sparklingpandas/sparklingpandas',
    license='LICENSE.txt',
    description='Enable Pandas on PySpark',
    long_description=open('README.md').read(),
    install_requires=[
        # Note: we also need PySpark 1.3 but that has to be installed manually.
        'pandas >= 0.13',
        'openpyxl>=1.6.1,<=2.0.0',
        'py4j'
    ],
    test_requires=[
        'nose',
        'coverage',
        'unittest2'
    ],
)
