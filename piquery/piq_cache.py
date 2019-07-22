from os import path, makedirs
from glob import glob
import json
import numpy as np
import redis
from piquery.piq_config import config as cfg

def saveJson(data, json_file):
    save_json = json.dumps(data)
    with open(json_file, 'w') as f:
        f.write(save_json)

def loadJson(json_file):
    with open(json_file, 'r') as f:
        res = json.loads(f.readlines()[0])
    return res

def saveCache(img_hash, hash_df, desf):
    for idx in hash_df.index:
        cid, _id, fp = hash_df.loc[idx, ['cid', 'id', 'fp']]
        f_dir = cfg['cache_dir'] + img_hash
        f_path = '{}/{}_{}.json'.format(f_dir, cid, _id)
        if path.isfile(f_path):
            continue
        if not path.isdir(f_dir):
            makedirs(f_dir)

        saveJson(desf(fp).tolist(), f_path)

def loadCache(img_hash):
    f_dir = cfg['cache_dir'] + img_hash
    all_files = glob(f_dir + '/*')
    for f_j in all_files:
        yield path.basename(f_j)[:-5].split('_'), np.array(loadJson(f_j), dtype=np.uint8)

class RedisCache:
    def __init__(self):
        self.r = redis.Redis(host=cfg['redis_host'], port=cfg['redis_port'], db=0)
    def write(self, k, v):
        try:
            self.r.set(k, v)
        except:
            return False
        return True
    def get(self, k):
        try:
            return str(self.r.get(k), 'utf-8')
        except:
            return None
