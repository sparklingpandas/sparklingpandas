from setuptools import setup

try:
    import pypandoc
    readme = pypandoc.convert('README.md', 'rest')
except (ImportError, IOError):
    print "Failed to import pypandoc or failed to load README. Using empty readme"
    readme = ""
setup(
    name='sparklingpandas',
    version='0.0.1',
    author='Holden Karau',
    author_email='holden@pigscanfly.ca',
    packages=['sparklingpandas', 'sparklingpandas.test'],
    url='https://github.com/holdenk/PandaSpark',
    license='LICENSE.txt',
    description='Enable Pandas on PySpark',
    long_description=readme,
    install_requires=[
        # Note: we also need PySpark 1.2 but that has to be installed manually.
        'pandas >= 0.13',
        'openpyxl>=1.6.1,<=2.0.0',
        'py4j',
        'pypandoc'
    ],
    test_requires=[
        'nose',
        'coverage',
        'unittest2'
    ],
)
