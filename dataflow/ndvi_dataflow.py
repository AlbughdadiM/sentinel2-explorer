import sys
from os.path import isfile,join,exists,isdir
from os import listdir
import os
from operator import add
import numpy as np
from functools import reduce
import rasterio 
from rasterio.io import MemoryFile
import argparse
import logging
import io

import apache_beam as beam
from apache_beam.pvalue import TaggedOutput

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystem import FileSystem


# from google.cloud import bigquery
from google.oauth2 import service_account
import googleapiclient.discovery
#from google.cloud import storage



logger = logging.getLogger()
logger.setLevel(logging.INFO)


BUCKET_NAME = 'gcp-public-data-sentinel-2'
MY_BUCKET = 'YOUR_PROJECT'
gs_secret_access_key = ''
gs_access_key = ''


class GCSFileReader:
  """Helper class to read gcs files"""
  def __init__(self, gcs):
      self.gcs = gcs

class ProcessOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):

        parser.add_argument(
            "--start_date",
            default='2017-02-02',
            help="First Image"
        )

        parser.add_argument(
            "--end_date",
            default='2017-02-12',
            help="Last Image"
        )

        parser.add_argument(
            "--tile",
            default='31TCJ',
            help="S2 Tile"
        )

        parser.add_argument(
            "--cloud_cover",
            default='20',
            help="Max Cloud Cover"
        )





class BiqQuery(beam.DoFn):

    

    def __init__(self, start,end,tile,cloud):
        self.start = start
        self.end = end
        self.tile = tile
        self.cloud = cloud

    def start_bundle(self):
        from google.cloud import bigquery
        self.client = bigquery.Client()
 
    def process(self,list):
        query = self.client.query("""
                    SELECT * FROM `bigquery-public-data.cloud_storage_geo_index.sentinel_2_index` 
                        WHERE (mgrs_tile = '{t}' AND 
                        CAST(SUBSTR(sensing_time, 1, 10) AS DATE) >= CAST('{s}' AS DATE) AND 
                        CAST(SUBSTR(sensing_time, 1, 10) AS DATE) < CAST('{e}' AS DATE))
                    """.format(t=self.tile, s=self.start, e=self.end))
        results = query.result()
        df = results.to_dataframe()
        good_scenes = []
        for _, row in df.iterrows():
            print (row['product_id'], '; cloud cover:', row['cloud_cover'])
            if float(row['cloud_cover']) <= self.cloud:
                good_scenes.append(row['base_url'].split(BUCKET_NAME)[1][1:])
        good_scenes = [(good_scenes[i],"Image_"+str(i+1)) for i in range (len(good_scenes))]
        print ("Result of Search",good_scenes)
        yield good_scenes

class GetBands(beam.DoFn):
    

    def start_bundle(self):
        from google.cloud import storage
        self.storage_client = storage.Client()
    def process(self,tuple_image):
        print ("Inside Get Bands",tuple_image)
        image_gcs = tuple_image[0]
        list_bands = []
        intermediate_fname = get_prefixes(self.storage_client,BUCKET_NAME,join(image_gcs,'GRANULE/'),'/')
        if intermediate_fname:
            intermediate_fname = intermediate_fname[0]
            bands_info = get_prefixes(self.storage_client,BUCKET_NAME,join(intermediate_fname,'IMG_DATA/'),'/')
            if bands_info:
                list_bands = get_bands_fname(bands_info)
        yield (image_gcs,list_bands)

class EstimateVI(beam.DoFn):
   

    def start_bundle(self):
        from google.cloud import storage
        self.storage_client = storage.Client()
    def process(self,tuple_bands):
        print ("THIS TUPLE",tuple_bands)
        list_bands = tuple_bands[1]
        estimate_ndsi(list_bands[7],list_bands[3],tuple_bands[0],self.storage_client)



def get_prefixes(storage_client,bucket_name,prefix,delimiter):
    iterator = storage_client.list_blobs(bucket_name,prefix=prefix,delimiter=delimiter)
    response = iterator._get_next_page_response()
    if 'prefixes' in response.keys():
        return_value = response['prefixes']
    elif 'items' in response.keys():
        return_value = response['items']
    else:
        print ("Can't find what you look for")
        sys.exit()
    return return_value

def get_bands_fname(bands_info):
    list_bands = []
    for band in bands_info:
        list_bands.append(join("gs://",'/'.join(band['id'].split('/')[:-1])))
    return list_bands

def read_band(band):
    print (band)
    os.environ["CURL_CA_BUNDLE"] = "/etc/ssl/certs/ca-certificates.crt"
    with rasterio.Env(GS_SECRET_ACCESS_KEY=gs_secret_access_key, GS_ACCESS_KEY_ID=gs_access_key):
        with rasterio.open(band,driver='JP2OpenJPEG') as src:
            data = src.read(1)
            profile = src.profile
    return data,profile


def estimate_ndsi(band1,band2,output_fname,storage_client):
    from google.cloud import storage
    band1_arr,dstprofile = read_band(band1)
    band2_arr,_ = read_band(band2)
    ndsi = np.divide((band1_arr-band2_arr),(band1_arr+band2_arr))
    bucket = storage_client.get_bucket(MY_BUCKET)
    blob = storage.Blob(output_fname, bucket)
    storage.blob._DEFAULT_CHUNKSIZE = 2097152 # 1024 * 1024 B * 2 = 2 MB
    storage.blob._MAX_MULTIPART_SIZE = 2097152 # 2 MB
    dstprofile.update(
        dtype=rasterio.float32,
        count=1,
        driver='GTiff',
        compress='lzw')
    with MemoryFile() as memfile:
        with memfile.open(**dstprofile) as dataset:
            dataset.write(ndsi.astype(rasterio.float32),1)
        blob.upload_from_file(memfile,MY_BUCKET)


if __name__ == "__main__":
    pipeline_options = PipelineOptions()
    p = beam.Pipeline(options=pipeline_options)
    process_options = pipeline_options.view_as(ProcessOptions)
    gcs = GCSFileSystem(pipeline_options)

    (p
    |"Create PCollection" >> beam.Create([None])
    |"Query" >> beam.ParDo(BiqQuery(process_options.start_date,process_options.end_date,process_options.tile,float(process_options.cloud_cover)))
    |"Flatten" >> beam.FlatMap(lambda elements: elements)
    |"Get Bands" >> beam.ParDo(GetBands())
    |"Estimate VI" >> beam.ParDo(EstimateVI())
    )
    p.run().wait_until_finish()

