import os
import sys
from os.path import isfile,isdir,join
import numpy as np
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import googleapiclient.discovery
import rasterio
from google.cloud import storage
from rasterio.io import MemoryFile



BUCKET_NAME = 'gcp-public-data-sentinel-2'
MY_BUCKET = 'YOUR_PROJECT'


def query_sentinel(start, end, tile, cloud=100.):
    client = bigquery.Client()
    query = client.query("""
                SELECT * FROM `bigquery-public-data.cloud_storage_geo_index.sentinel_2_index` 
                    WHERE (mgrs_tile = '{t}' AND 
                    CAST(SUBSTR(sensing_time, 1, 10) AS DATE) >= CAST('{s}' AS DATE) AND 
                    CAST(SUBSTR(sensing_time, 1, 10) AS DATE) < CAST('{e}' AS DATE))
                """.format(t=tile, s=start, e=end))
    results = query.result()
    df = results.to_dataframe()
    good_scenes = []
    for _, row in df.iterrows():
        print (row['product_id'], '; cloud cover:', row['cloud_cover'])
        if float(row['cloud_cover']) <= cloud:
            good_scenes.append(row['base_url'].split(BUCKET_NAME)[1][1:])
    return good_scenes

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
    #with rasterio.Env(GS_SECRET_ACCESS_KEY=gs_secret_access_key, GS_ACCESS_KEY_ID=gs_access_key):
    with rasterio.open(band,driver='JP2OpenJPEG') as src:
        data = src.read(1)
        profile = src.profile
    return data,profile


def estimate_ndsi(band1,band2,output_fname,client):
    band1_arr,dstprofile = read_band(band1)
    band2_arr,_ = read_band(band2)
    ndsi = np.divide((band1_arr-band2_arr),(band1_arr+band2_arr))
    bucket = client.get_bucket(MY_BUCKET)
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


def run_fun(start_date,end_date,tile,cloud_cover,output_path):
    list_scenes = query_sentinel(start_date,end_date,tile,cloud_cover)
    storage_client = storage.Client()
    list_bands_scene = {}
    for scene in list_scenes[2:3]:
        print (scene)
        intermediate_fname = get_prefixes(storage_client,BUCKET_NAME,join(scene,'GRANULE/'),'/')
        if intermediate_fname:
            intermediate_fname = intermediate_fname[0]
            bands_info = get_prefixes(storage_client,BUCKET_NAME,join(intermediate_fname,'IMG_DATA/'),'/')
            if bands_info:
                list_bands = get_bands_fname(bands_info)
                list_bands_scene[scene] = list_bands
                ndsi_fname = scene.split('/')[-1].split('.')[0]+'.tif'
                estimate_ndsi(list_bands[7],list_bands[3],ndsi_fname,storage_client)
            else:
                list_bands_scene[scene] = []
        else:
            list_bands_scene[scene] = []
    return list_bands_scene
