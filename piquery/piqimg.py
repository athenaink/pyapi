import requests
from PIL import Image
from io import BytesIO
import numpy as np
import cv2 as cv

from piquery.piq_feature import ImFeature, CropImFeature, ResizeImFeature, ImSim
from piquery.piq_hash import imhash_dct

class ImgDownloader:
    @staticmethod
    def download(url):
        headers = headers = { 'user-agent':
                             'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36' }

        return requests.get(url, timeout=3.0, headers=headers)

    @staticmethod
    def download_numpy(url):
        res = ImgDownloader.download(url)
        return np.asarray(Image.open(BytesIO(res.content)))

class ImgTransformer:
    @staticmethod
    def rgb2bgr(img_data):
        return img_data[:, :, [2, 1, 0]]
    @staticmethod
    def bgr2gray(img_data):
        return cv.cvtColor(img_data, cv.COLOR_BGR2GRAY)

class ImgFeature:
    def __init__(self):
        self.imf = ImFeature(k=50)
        self.imf_crop = CropImFeature(k=50)
        self.imf_resize = ResizeImFeature(k=50)
        self.imsim = ImSim(k=50, crop=False)
    def gray2hash(self, gray):
        return imhash_dct(self.imf_resize.resize(self.imf_crop.crop(gray), size=4))
    def gray2des(self, gray):
        return self.imf.feature(gray)[1]
    def des2fp(self, des):
        return self.imf.fingerprint(des)
    def fp2des(self, fp):
        kp_num = int(len(fp) / (64))
        ut8arr = np.array([int(fp[i:i+2], 16) for i in range(0, len(fp), 2)], dtype=np.uint8)
        return ut8arr.reshape(kp_num, 32)
    def sim(self, a, b):
        return self.imsim.calcSim(a, b)