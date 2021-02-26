import os
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from xml.etree import ElementTree as ET


BASE_URL = 'http://storage.googleapis.com/'
l2a_catalog = 'YOUR_PROJECT.s2_l2a_index.catalog'
l1c_catalog = 'bigquery-public-data.cloud_storage_geo_index.sentinel_2_index'

def query_sentinel(key_json, project_id, start, end, tile, cloud=100.,level='l2a'):
    if level=='l2a' or level=='L2A':
        catalog = l2a_catalog
        s_t = 'SENSING_TIME'
        p_id = 'PRODUCT_ID'
        c_c = 'CLOUD_COVER'
        b_url = 'BASE_URL'
    else:
        catalog=l1c_catalog
        s_t = 'sensing_time'
        p_id = 'product_id'
        c_c = 'cloud_cover'
        b_url = 'base_url'
    credentials = service_account.Credentials.from_service_account_file(key_json)
    client = bigquery.Client(credentials=credentials, project=project_id)
    query = client.query("""
                SELECT * FROM `{c}` 
                    WHERE (mgrs_tile = '{t}' AND 
                    CAST(SUBSTR(STRING({st}), 1, 10) AS DATE) >= CAST('{s}' AS DATE) AND 
                    CAST(SUBSTR(STRING({st}), 1, 10) AS DATE) < CAST('{e}' AS DATE))
                """.format(c=catalog,t=tile,st= s_t,s=start, e=end))
    results = query.result()
    df = results.to_dataframe()
    df = df.drop_duplicates()
    good_scenes = []
    for i, row in df.iterrows():
        print (row[p_id], '; cloud cover:', row[c_c])
        if float(row[c_c]) <= cloud:
            good_scenes.append(row[b_url].replace('gs://', BASE_URL))
    return good_scenes


def download_file(url, dst_name):
    try:
        data = requests.get(url, stream=True)
        with open(dst_name, 'wb') as out_file:
            for chunk in data.iter_content(chunk_size=100 * 100):
                out_file.write(chunk)
    except:
        print ('\t ... {f} FAILED!'.format(f=url.split('/')[-1]))
    return

def make_safe_dirs(scene, outpath,processing_level):
    scene_name = os.path.basename(scene)
    scene_path = os.path.join(outpath, scene_name)
    manifest = os.path.join(scene_path, 'manifest.safe')
    manifest_url = scene + '/manifest.safe'
    if os.path.exists(manifest):
        os.remove(manifest)
    download_file(manifest_url, manifest)
    mtd_xml = os.path.join(scene_path,'MTD_MSI'+processing_level+'.xml')
    mtd_url = scene+'/MTD_MSI'+processing_level+'.xml'
    if os.path.exists(mtd_xml):
        os.remove(mtd_xml)
    download_file(mtd_url,mtd_xml)
    xml_tree = ET.parse(mtd_xml)
    bands_path = []
    for elem in xml_tree.iter():
        if 'IMAGE_FILE' in elem.tag:
            bands_path.append(elem.text+'.jp2')

    with open(manifest, 'r') as f:
        manifest_lines = f.read().split()
    download_links = []
    load_this = False
    for line in manifest_lines:
        if 'href' in line:
            online_path = line[7:line.find('><')]
            online_path = online_path.split('"')[0]
            #tile = scene_name.split('_')[-2]
            if online_path.startswith('/GRANULE/'):
                None
            else:
                load_this = True
            if load_this:
                local_path = os.path.join(scene_path, *online_path.split('/')[1:])
                online_path = scene + online_path
                download_links.append((online_path, local_path))
        load_this = False
    for b_p in bands_path:
        local_path = os.path.join(scene_path,b_p)
        online_path = os.path.join(scene,b_p)
        download_links.append((online_path, local_path))
    for extra_dir in ('AUX_DATA', 'HTML'):
        if not os.path.exists(os.path.join(scene_path, extra_dir)):
            os.makedirs(os.path.join(scene_path, extra_dir))
    return download_links

def download_sentinel(scene, dst,processing_level):
    scene_name = scene.split('/')[-1]
    scene_path = os.path.join(dst, scene_name)
    if not os.path.exists(scene_path):
        os.mkdir(scene_path)
    print ('Downloading scene {s} ...'.format(s=scene_name))
    download_links = sorted(make_safe_dirs(scene, dst,processing_level))
    for l in download_links:
        if not os.path.exists(os.path.dirname(l[1])):
            os.makedirs(os.path.dirname(l[1]))
        if os.path.exists(l[1]):
            os.remove(l[1])
        if l[1].endswith('.jp2'):
            print ('\t ... *{b}'.format(b=l[1].split('_')[-1]))
        if download_file(l[0], l[1]) is False:
            print ('\t ... {f} failed to download! Download for this scene is cancelled here!'.format(f=l[0]))
            return
def run_download_gcs(key_json,project_id,tile,start_date,end_date,cloud,output_path,processing_level):
    scene_list = query_sentinel(key_json,project_id,start_date,end_date,tile,cloud,level=processing_level)
    for s in scene_list:
        download_sentinel(s,output_path,processing_level)


if __name__ == '__main__':
    key_json = 'sa_file.json'
    project_id = 'YOUR_PROJECT'
    output_path = '/tmp'
    tile = '31TFJ'
    start_date = '2017-08-01'
    end_date = '2017-08-25'
    processing_level = 'L2A'
    cloud = 20
    run_download_gcs(key_json,project_id,tile,start_date,end_date,cloud,output_path,processing_level)