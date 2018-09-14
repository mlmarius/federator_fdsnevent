# Always prefer setuptools over distutils
from setuptools import setup, find_packages

setup(
    name='effdsnevent',  # Required
    version='0.0.1',  # Required
    description='Epos Federation for the FDSN Event APIs',  # Required
    package_dir={'': 'src'},
    packages=find_packages('src'),
    entry_points={'eposfederator.federators': 'effdsnevent = effdsnevent'},
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    zip_safe=False,
    package_data={'effdsnevent': ['settings.yml']}
)
