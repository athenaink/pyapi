import cv2 as cv
import numpy as np
import pandas as pd
import os

def parseKeypoints(kp):
    return [{'center': p.pt,
          'diameter': p.size,
          'angle': p.angle,
          'class_id': p.class_id,
          'octave': p.octave,
          'response': p.response} for p in kp]

def kpdfsort(kp):
    return pd.DataFrame(parseKeypoints(kp)).sort_values(by=['response'], ascending=False)[['center', 'diameter', 'angle', 'response']]

def orbparams(orb):
    params = dict()
    params['DefaultName'] = orb.getDefaultName()
    params['EdgeThreshold'] = orb.getEdgeThreshold()
    params['FastThreshold'] = orb.getFastThreshold()
    params['FirstLevel'] = orb.getFirstLevel()
    params['MaxFeatures'] = orb.getMaxFeatures()
    params['NLevels'] = orb.getNLevels()
    params['PatchSize'] = orb.getPatchSize()
    params['ScaleFactor'] = orb.getScaleFactor()
    params['ScoreType'] = orb.getScoreType()
    params['WTA_K'] = orb.getWTA_K()

    return params

def byte2hex(bt):
    hx = hex(bt).split('x')[1]
    if bt < 16:
        return '0' + hx
    return hx

# 解决中文路径问题
def cv_imread(file_path):
    root_dir, file_name = os.path.split(file_path)
    pwd = os.getcwd()
    if root_dir:
        os.chdir(root_dir)
    cv_img = cv.imread(file_name)
    os.chdir(pwd)
    return cv_img

class ImFeature:
    def __init__(self, alg=None, k=500):
        if alg == 'sift':
            self.algf = cv.xfeatures2d.SIFT_create()
        elif alg == 'surf':
            self.algf = cv.xfeatures2d.SURF_create()
        else:
            self.algf = cv.ORB_create(k)
        self.alg = alg
        self.matcher = None
        self.flann_matcher = None
        self.store = dict()
    def read(self, img_path):
        # if not img_path in self.store:
            # store = self.store
        # store[img_path] = dict()
        bgr = cv_imread(img_path)
        gray= cv.cvtColor(bgr, cv.COLOR_BGR2GRAY)
            # store[img_path]['bgr'] = bgr
            # store[img_path]['gray'] = gray
        return bgr, gray
    def keypoint(self, im):
        if isinstance(im, str):
            bgr, gray = self.read(im)
            return gray, self.algf.detect(gray, None)
        elif isinstance(im, np.ndarray):
            return im, self.algf.detect(im, None)
        return None, None
    def descriptor(self, img, kp):
        return self.algf.compute(img, kp)
    def fingerprint(self, descriptor):
        return ''.join([''.join([byte2hex(d) for d in dps]) for dps in descriptor])
    def feature(self, im):
        if isinstance(im, str):
            bgr, gray = self.read(im)
            return self.algf.detectAndCompute(gray, None)
        elif isinstance(im, np.ndarray):
            return self.algf.detectAndCompute(im, None)
        return None, None
    def fastFeature(self, im):
        bgr, gray = self.read(im)
        fast = cv.FastFeatureDetector_create()
        kp = fast.detect(gray, None)
        return kp
    def match(self, im1, im2, k=None):
        kp1, des1 = self.feature(im1)
        kp2, des2 = self.feature(im2)
        alg = self.alg
        if self.matcher is None:
            if alg == 'sift':
                self.matcher = cv.BFMatcher()
            elif alg == 'surf':
                self.matcher = cv.BFMatcher()
            else:
                self.matcher = cv.BFMatcher(cv.NORM_HAMMING, crossCheck=True)
        if k is None:
            return self.matcher.match(des1, des2)
        else:
            return self.matcher.knnMatch(des1,des2, k)
    def flannMatch(self, im1, im2):
        if isinstance(im1, str):
            kp1, des1 = self.feature(im1)
            kp2, des2 = self.feature(im2)
        else:
            des1 = im1
            des2 = im2
        alg = self.alg
        if self.flann_matcher is None:
            if alg == 'sift' or alg == 'surf':
                FLANN_INDEX_KDTREE = 1
                index_params = dict(algorithm = FLANN_INDEX_KDTREE, trees = 5)
                search_params = dict(checks=50)
                self.flann_matcher = cv.FlannBasedMatcher(index_params,search_params)
            else:
                FLANN_INDEX_LSH = 6
                index_params= dict(algorithm = FLANN_INDEX_LSH,
                    table_number = 6, # 12
                    key_size = 12,     # 20
                    multi_probe_level = 1) #2
                search_params = dict(checks=50)
                self.flann_matcher = cv.FlannBasedMatcher(index_params,search_params)
        if alg == 'sift' or alg == 'surf':
            return self.flann_matcher.knnMatch(des1,des2,k=2)
        else:
            return self.flann_matcher.match(des1, des2)

class CropImFeature(ImFeature):
    def crop(self, img):
        h, w = img.shape[:2]
        if h > w:
            offset = int((h - w)/2)
            limit = offset + w
            if limit > h:
                limit = h
            return img[offset:limit, :]
        elif h < w:
            offset = int((w - h)/2)
            limit = offset + h
            if limit > w:
                limit = w
            return img[:, offset:limit]
        return img
    def read(self, img_path, size=100):
        bgr, gray = super().read(img_path)
        return bgr, self.crop(gray)

class ResizeImFeature(CropImFeature):
    def resize(self, img, size=128):
        h, w = img.shape[:2]
        resize_w = size
        resize_h = int(h * resize_w/w)
        return cv.resize(img, (resize_w, resize_h), interpolation = cv.INTER_CUBIC)
    def read(self, img_path, size=100):
        bgr, gray = super().read(img_path)
        return bgr, self.resize(gray, size=size)

class ImSim:
    def __init__(self, k=500, resize=False, crop=True):
        self.k = k
        if resize == True:
            self.feature = ResizeImFeature(k=k)
        elif crop == True:
            self.feature = CropImFeature(k=k)
        else:
            self.feature = ImFeature(k=k)
    def match(self, img1, img2):
        matches = self.feature.flannMatch(img1, img2)
        return sorted([match for match in matches if match.distance < 10], key=lambda x:x.distance)
    def getFeature(self, img):
        return self.feature.feature(img)
    def calcSim(self, img1, img2):
        k = self.k
        if isinstance(img1, str):
            kp1, des1 = self.feature.feature(img1)
            kp2, des2 = self.feature.feature(img2)
            if len(kp1) > len(kp2):
                k = len(kp2)
            else:
                k = len(kp1)
            matches = self.match(des1, des2)
        else:
            if len(img1) > len(img2):
                k = len(img2)
            else:
                k = len(img1)
            matches = self.match(img1, img2)
        return len(matches)/k