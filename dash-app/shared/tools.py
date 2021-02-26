import os
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from shapely.geometry import box,Polygon
import geopandas as gpd
import json
from xml.etree import ElementTree as ET




BASE_URL = 'http://storage.googleapis.com/'
project_id = 'YOUR_PROJECT'
key_json = os.path.join(os.getcwd(),'shared','sa_file.json')
l2a_catalog = 'YOUR_PROJECT.s2_l2a_index.catalog'
l1c_catalog = 'bigquery-public-data.cloud_storage_geo_index.sentinel_2_index'

def bytesto(in_bytes, to, bsize=1024): 
    a = {'k' : 1, 'm': 2, 'g' : 3, 't' : 4, 'p' : 5, 'e' : 6 }
    return in_bytes / (bsize ** a[to])

def query_sentinel(start, end, tile, cloud=100.,level='l2a'):
    if level=='l2a':
        catalog = l2a_catalog
        s_t = 'SENSING_TIME'
        p_id = 'PRODUCT_ID'
        c_c = 'CLOUD_COVER'
        b_url = 'BASE_URL'
        g_c_f = 'GEOMETRIC_QUALITY_FLAG'
        tot_s = 'TOTAL_SIZE'
        w_lon = 'WEST_LON'
        s_lat = 'SOUTH_LAT'
        e_lon = 'EAST_LON'
        n_lat = 'NORTH_LAT'
    else:
        catalog=l1c_catalog
        s_t = 'sensing_time'
        p_id = 'product_id'
        c_c = 'cloud_cover'
        b_url = 'base_url'
        g_c_f = 'geometric_quality_flag'
        tot_s = 'total_size'
        w_lon = 'west_lon'
        s_lat = 'south_lat'
        e_lon = 'east_lon'
        n_lat = 'north_lat'
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
    polygon_list = []
    sum_total_size = 0.0
    sum_cloud_cover = 0.0
    count_v_geometry = 0
    for i, row in df.iterrows():
        if float(row[c_c]) <= cloud:
            if row[g_c_f] == 'PASSED':
                count_v_geometry+=1
            good_scenes.append(row[b_url].replace('gs://', BASE_URL))
            sum_cloud_cover+=float(row[c_c])
            sum_total_size+=float(row[tot_s])
            bounds_polygon = box(row[w_lon], row[s_lat],
                row[e_lon], row[n_lat])
            polygon_list.append(bounds_polygon)
    d = {'image': good_scenes, 'geometry': polygon_list}
    poly_gdf = gpd.GeoDataFrame(d, crs="EPSG:4326")
    avg_cloud_cover = sum_cloud_cover/(i+1)
    sum_total_size = bytesto(sum_total_size,'g')
    return avg_cloud_cover,"%.2f" %sum_total_size,count_v_geometry,poly_gdf



def query_sentinel_with_polygon(positions,start,end,cloud=100.,level='l2a'):
    if level=='l2a':
        catalog = l2a_catalog
        s_t = 'SENSING_TIME'
        p_id = 'PRODUCT_ID'
        c_c = 'CLOUD_COVER'
        b_url = 'BASE_URL'
        g_c_f = 'GEOMETRIC_QUALITY_FLAG'
        tot_s = 'TOTAL_SIZE'
        w_lon = 'WEST_LON'
        s_lat = 'SOUTH_LAT'
        e_lon = 'EAST_LON'
        n_lat = 'NORTH_LAT'
    else:
        catalog=l1c_catalog
        s_t = 'sensing_time'
        p_id = 'product_id'
        c_c = 'cloud_cover'
        b_url = 'base_url'
        g_c_f = 'geometric_quality_flag'
        tot_s = 'total_size'
        w_lon = 'west_lon'
        s_lat = 'south_lat'
        e_lon = 'east_lon'
        n_lat = 'north_lat'
    credentials = service_account.Credentials.from_service_account_file(key_json)
    client = bigquery.Client(credentials=credentials, project=project_id)
    polygon = Polygon(positions)
    query = client.query("""
    SELECT
      *
    FROM
        `{c}`
    WHERE
        (
        CAST(SUBSTR(STRING({st}), 1, 10) AS DATE) >= CAST('{s}' AS DATE) AND 
        CAST(SUBSTR(STRING({st}), 1, 10) AS DATE) < CAST('{e}' AS DATE) AND
        ST_INTERSECTS(ST_MakePolygon(ST_MakeLine([ ST_GeogPoint(west_lon,south_lat),ST_GeogPoint(west_lon,north_lat),ST_GeogPoint(east_lon,north_lat),ST_GeogPoint(east_lon,south_lat)])),ST_GeogFromText('{t}')))
        """.format(c=catalog,st=s_t,t=polygon.wkt, s=start, e=end))
    results = query.result()
    df = results.to_dataframe()
    df = df.drop_duplicates()
    good_scenes = []
    polygon_list = []
    sum_total_size = 0.0
    sum_cloud_cover = 0.0
    count_v_geometry = 0
    for i, row in df.iterrows():
        #print (row['product_id'], '; cloud cover:', row['cloud_cover'])
        if float(row[c_c]) <= cloud:
            if row[g_c_f] == 'PASSED':
                count_v_geometry+=1
            good_scenes.append(row[b_url].replace('gs://', BASE_URL))
            sum_cloud_cover+=float(row[c_c])
            sum_total_size+=float(row[tot_s])
            bounds_polygon = box(row[w_lon], row[s_lat],
                row[e_lon], row[n_lat])
            polygon_list.append(bounds_polygon)
    d = {'image': good_scenes, 'geometry': polygon_list}
    poly_gdf = gpd.GeoDataFrame(d, crs="EPSG:4326")
    avg_cloud_cover = sum_cloud_cover/(i+1)
    sum_total_size = bytesto(sum_total_size,'g')
    return avg_cloud_cover,"%.2f" %sum_total_size,count_v_geometry,poly_gdf

def download_file(url, dst_name):
    try:
        data = requests.get(url, stream=True)
        with open(dst_name, 'wb') as out_file:
            for chunk in data.iter_content(chunk_size=100 * 100):
                out_file.write(chunk)
    except:
        print ('\t ... {f} FAILED!'.format(f=url.split('/')[-1]))
    return

def make_safe_dirs(scene,processing_level):
    mtd_url = scene+'/MTD_MSI'+processing_level+'.xml'
    raw_mtd = requests.get(mtd_url,stream=True)
    mtd_file = raw_mtd.content
    mtd_file = str(mtd_file,'utf-8')
    xml_tree = ET.fromstring(mtd_file)
    download_links = []
    for elem in xml_tree.iter():
        if 'IMAGE_FILE' in elem.tag:
            download_links.append(os.path.join(scene,elem.text+'.jp2'))
    return download_links