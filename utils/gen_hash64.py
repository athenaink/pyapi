from glob import glob
from os import path, makedirs
from time import time
import pandas as pd
import argparse
from shutil import move
from PIL import Image

def dHashBin(imgfile, size=(8, 9)):
    im = Image.open(imgfile)
    pixels = im.resize(size, Image.ANTIALIAS).load()
    bin_list = list()
    h, w = size
    for i in range(h):
        for j in range(w-1):
            b = '0' if pixels[i, j] < pixels[i, j+1] else '1'
            bin_list.append(b)

    return bin_list

def binlist2int(l):
    return int(''.join(l), 2)

def dhash64(imgfile):
    return binlist2int(dHashBin(imgfile))

def load_image_datasets(images_dir):
    images = glob(images_dir + '/*/*')

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
        for url, cid, aid, dhash in datasets:
            line = '{},{},{},{}'.format(url, cid, aid, dhash)
            f.write('{}\n'.format(line))
        f.close()

def hash_stat(datasets_file, hash_name='dhash', names=['url', 'cid', 'aid', 'dhash']):
    df = pd.read_csv(datasets_file, names=names, header=None)
    hash_df = df.sort_values(hash_name).groupby(hash_name)[['aid']].count().sort_values('aid', ascending=False)
    hash_df = pd.DataFrame({'hash': hash_df.index, 'count': hash_df['aid']}).reset_index(drop=True)
    total = hash_df['count'].sum()
    hash_df['percent(%)'] = round(hash_df['count'] * 100 / total, 2)
    return hash_df

def gen_image_hash_datasets(images_dir, save_path, batch_size):
    start_time = time()
    print('loading image datasets ...')
    datasets_raw = load_image_datasets(images_dir)
    print('generating image hash datasets ({}) ...'.format(len(datasets_raw)))
    gen_count = 0
    batch_time = time()
    datasets = list()
    proced_dir = '{}/images'.format(path.split(images_dir.rstrip('/'))[0])
    if not path.isdir(proced_dir):
        makedirs(proced_dir)
    for x in datasets_raw:
        try:
            img_path = x[0]
            if not path.isfile(img_path):
                continue
            row = (*x, dhash64(img_path))
            datasets.append(row)
            # 转换过的文件移动到新目录
            move(img_path, proced_dir)
            gen_count += 1
        except:
            print('{} fail to generate hash'.format(img_path))

        if gen_count % batch_size == 0:
            print('{}\t{} s'.format(gen_count, round(time() - batch_time, 1)))
            batch_time = time()
    print('saving image hash datasets to csv file ...')
    save_datasets(datasets, save_path)

    print('counting hash ...')
    tmp = list(path.splitext(save_path))
    tmp.insert(1, '.stat')
    hash_datasets_stat_path = ''.join(tmp)

    hash_stat_df = hash_stat(save_path)
    hash_stat_df.to_csv(hash_datasets_stat_path, index=False)

    print('------\ncompleted!')
    print('size {} cost time {} s'.format(gen_count, round(time() - start_time, 1)))
    return hash_stat_df.head()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--indir', type=str, default='/data/kdd/cache/art_gray_256', 
                        help='directory of images')
    parser.add_argument('-o', '--out', type=str, default='/data/kdd/cache/hash64_datasets.csv', 
                        help='datasets csv file output or save path')
    parser.add_argument('-b', '--batch', type=int, default=10000, 
                        help='batch size to print')
    args = parser.parse_args()

    print(gen_image_hash_datasets(args.indir, args.out, args.batch))
