from time import time
from piquery.piq_feature import ResizeImFeature
import pandas as pd
import numpy as np
import cv2 as cv

def imhash(gray):
    dt = gray.flatten()
    xlen = len(dt)
    avg = dt.mean()
    avg_list = ['0' if i < avg else '1' for i in dt]
    return ''.join(['%x' % int(''.join(avg_list[x: x+4]), 2) for x in range(0, xlen, 4)])

def imhash_dct(gray):
    h, w = gray.shape[:2]
    vis0 = np.zeros((h,w), np.float32)
    vis0[:h,:w] = gray       #填充数据

    vis1 = cv.dct(cv.dct(vis0))

    return imhash(vis1)

def hamming(hash1, hash2):
    len1 = len(hash1)
    len2 = len(hash2)
    hlen = 0
    if len1 < len2:
        hlen = len1
    else:
        hlen = len2
    if hlen == 0:
        return -1
    distance = 0
    for i in range(hlen):
        num1 = int(hash1[i], 16)
        num2 = int(hash2[i], 16)
        xor_num = num1 ^ num2
        for i in [1, 2, 4, 8]:
            if i & xor_num != 0:
                distance += 1
    return distance

class PIQHash:
    def __init__(self, df_hash):
        self.imf_resize = ResizeImFeature(k=50)
        self.df_hash = df_hash
    def getHash(self, im, hash_k):
        if isinstance(im, str):
            bgr, gray = self.imf_resize.read(im, hash_k * 2)
        else:
            gray = im
        return imhash_dct(gray)
    def query(self, hash, hash_k):
        hashes = self.df_hash['hash_k' + str(hash_k)]
        res = list()
        # start = time()
        for i, hk in enumerate(hashes):
            if hash == hk:
                res.append(i)
        # print('query hash cost time: {} s'.format(time() - start))

        return res

    def queryHamming(self, hash, hash_k):
        hashes = self.df_hash['hash_k' + str(hash_k)]
        res = list()
        distances = list()
        # start = time()
        for i, hk in enumerate(hashes):
            distance = hamming(hash, hk)
            if distance < 5:
                distances.append((i, distance))
        # print('query hamming cost time: {} s'.format(time() - start))

        return [d[0] for d in sorted(distances, key=lambda x:x[1])]