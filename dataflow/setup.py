from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

setup(
    name='process',
    version='0.0.1',
    packages=find_packages(),
    install_requires=[
        'pandas==1.1.2',
        'rasterio',
        'numpy',
        'pyarrow==2.0.0',
        'pandas',
        'rasterio',
        'numpy',
        'pyarrow',
        'apache-beam[gcp]==2.27.0',
        'google-api-core==1.24.1',
        'google-api-python-client==1.12.8',
        'google-apitools==0.5.31',
        'google-auth==1.21.1',
        'google-auth-httplib2==0.0.4',
        'google-auth-oauthlib==0.4.2',
        'google-cloud==0.34.0',
        'google-cloud-bigquery==1.28.0',
        'google-cloud-bigtable==1.6.1',
        'google-cloud-build==2.0.0',
        'google-cloud-core==1.5.0',
        'google-cloud-datastore==1.15.3',
        'google-cloud-dlp==1.0.0',
        'google-cloud-language==1.3.0',
        'google-cloud-pubsub==1.7.0',
        'google-cloud-spanner==1.19.1',
        'google-cloud-storage==1.35.0',
        'google-crc32c==1.1.0',
        'google-pasta==0.2.0',
        'google-resumable-media==1.2.0',
        'googleapis-common-protos==1.52.0',
                'apache-beam[gcp]==2.27.0'
            ]
)


