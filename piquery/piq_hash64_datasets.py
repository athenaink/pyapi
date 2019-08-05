from glob import glob
from os import path, makedirs, remove
from shutil import copy, move
from time import time
from piq_hash64 import ahash64, dhash64, phash64
import pandas as pd

def load_image_datasets(images_dir):
    images = glob(images_dir + '/*/*')
    # images = [f.replace('\\', '/') for f in images]

    extract_cid_aid = lambda x: path.basename(x).split('.')[0].split('_')
    image_datasets = [(x, *extract_cid_aid(x)) for x in images]
    return image_datasets

def save_datasets(datasets, save_path):
    if len(datasets) == 0:
        print('save empty datasets!')
        return
    save_dir = path.dirname(save_path)
    if save_dir and (not path.isdir(save_dir)):
        makedirs(save_dir)
    with open(save_path, 'a+', encoding='utf-8') as f:
        for url, cid, aid, ahash, dhash, phash in datasets:
            line = '{},{},{},{},{},{}'.format(url, cid, aid, ahash, dhash, phash)
            f.write('{}\n'.format(line))
        f.close()

def hash_stat(datasets_file, hash_name='dhash', names=['url', 'cid', 'aid', 'ahash', 'dhash', 'phash']):
    df = pd.read_csv(datasets_file, names=names, header=None)
    hash_df = df.sort_values(hash_name).groupby(hash_name)[['aid']].count().sort_values('aid', ascending=False)
    hash_df = pd.DataFrame({'hash': hash_df.index, 'count': hash_df['aid']}).reset_index(drop=True)
    total = hash_df['count'].sum()
    hash_df['percent(%)'] = round(hash_df['count'] * 100 / total, 2)
    return hash_df

def gen_image_hash_datasets(images_dir='/data/kdd/cache/art_gray_256', save_path='/data/kdd/cache/hash64_datasets.csv'):
    start_time = time()
    print('loading image datasets ...')
    datasets_raw = load_image_datasets(images_dir)
    print('generating image hash datasets ...')
    datasets = [(*x, ahash64(x[0]), dhash64(x[0]), phash64(x[0])) for x in datasets_raw]
    print('saving image hash datasets to csv file ...')
    save_datasets(datasets, save_path)
    print('------\ncompleted!')
    print('cost time {} s'.format(time() - start_time))
    tmp = list(path.splitext(save_path))
    tmp.insert(1, '.stat')
    hash_datasets_stat_path = ''.join(tmp)
    hash_stat_df = hash_stat(save_path)
    hash_stat_df.to_csv(hash_datasets_stat_path, index=False)
    return hash_stat_df.head()
