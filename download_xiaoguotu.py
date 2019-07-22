import requests
from os import path, makedirs
import hashlib
import pandas as pd

def md5(s):
    if isinstance(s, str):
        return hashlib.md5(s.encode(encoding='utf-8')).hexdigest()
    else:
        try:
            s = str(s)
        except:
            return None
        return hashlib.md5(s.encode(encoding='utf-8')).hexdigest()

image_url_prefix = 'http://pic2.to8to.com'
# data_rootdir = '/data/kdd/cache'
data_rootdir = 'download/kdd/cache'

def save_failed(failed_file, img_url):
    with open(failed_file,'a+') as f:
        f.write('{}\n'.format(img_url))
        f.close()

def append_map(map_file, k, v):
    with open(map_file,'a+') as f:
        f.write('{} {}\n'.format(k, v))
        f.close()

def download_img_by_filename(filename, thread_id=None, timeout=3):
    img_url_path = 'case/{}'.format(filename)
    img_url = '{}/{}'.format(image_url_prefix, img_url_path)
    print('{} downloading {} ...'.format(thread_id, img_url))
    try:
        res = requests.get(img_url, timeout=timeout)
    except:
        print('download {} timeout!\n'.format(img_url))
        save_failed('{}/{}-download_failed'.format(data_rootdir, thread_id), img_url)
        return
    
    if res.status_code != 200:
        print('download failed, with status code {}\n'.format(res.status_code))
        save_failed('{}/{}-download_failed'.format(data_rootdir, thread_id), img_url)
        return

    # image_ext = path.splitext(filename)[1]
    image_filename = md5(img_url_path)
    image_dir = '{}/{}'.format(data_rootdir, image_filename[:3])
    image_path = '{}/{}'.format(image_dir, image_filename)

    print('saving to {}'.format(image_path))

    if not path.isdir(image_dir):
        makedirs(image_dir)

    with open(image_path,'wb') as f:
        f.write(res.content)
        f.close()

    append_map('{}/{}-image.map'.format(data_rootdir, thread_id), img_url, image_path)
    print('saved.\n')

if __name__ == '__main__':
    csv_f = pd.read_csv('ods_to8to_to8to_to8to_xiaoguotu.csv', names=['cid', 'id', 'filename'], header=None)
    for filename in csv_f['filename']:
        download_img_by_filename(filename)
