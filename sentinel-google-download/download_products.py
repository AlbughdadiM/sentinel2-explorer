import os
import requests
import pandas as pd
from xml.etree import ElementTree as ET

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

def run_download_gcs(input_file,output_path,processing_level):
    df = pd.read_csv(input_file,sep=';')
    for _,row in df.iterrows():
        print (row)
        scene = row['image']
        download_sentinel(scene,output_path,processing_level)

if __name__ == '__main__':
    run_download_gcs('/home/mohanad/Downloads/7N0SX22B.csv','/home/mohanad/Desktop','L2A')


