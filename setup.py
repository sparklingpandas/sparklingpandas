from distutils.core import setup

setup(
    name='sparklingpandas',
    version='0.0.1',
    author='Holden Karau',
    author_email='holden@pigscanfly.ca',
    packages=['sparklingpandas', 'sparklingpandas.test'],
    url='https://github.com/holdenk/PandaSpark',
    license='LICENSE.txt',
    description='Enable Pandas on PySpark',
    long_description=open('README.md').read(),
    install_requires=[
        # Note: we also need PySpark but that has to be installed manually.
        'pandas >= 0.13',
    ],
)
