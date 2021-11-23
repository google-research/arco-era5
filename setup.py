from setuptools import setup, find_packages

setup(
    name='era5-to-zarr',
    packaging=find_packages(),
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    description="Scripts to convert Alphabet's copy of Era 5 to Zarr.",
    platforms=['darwin', 'linux'],
    python_requires='>=3.7, <3.9',
    install_requires=[
        'apache_beam[gcp]',
        'pangeo-forge-recipes @ git+https://github.com/alxmrs/pangeo-forge-recipes.git@release#egg=pangeo-forge-recipes',
        'gcsfs',
        'cfgrib',
    ],
    tests_require=['pytest'],
)