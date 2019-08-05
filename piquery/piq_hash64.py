import numpy as np
from PIL import Image
from io import BytesIO
import cv2
import requests
from piquery.piq_error import DownloadError, ImageFormatError

def download_to_hash(img_url, size=(256, 256)):
    try:
        res = requests.get(img_url)
    except:
        raise DownloadError('image downloading timeout!')

    if res.status_code != 200:
        raise DownloadError("image doesn't exist!")

    try:
        im = Image.open(BytesIO(res.content))
        im.thumbnail(size, Image.ANTIALIAS)
        im = im.convert('L' )
    except:
        raise ImageFormatError("image format not supported!")

    return binlist2int(dhash(im))

def aHashBin(imgfile, size=(8, 8)):
    im = Image.open(imgfile)
    im_data = im.resize(size, Image.ANTIALIAS).getdata()
    avg = np.array(im_data).mean()
    return ['0' if x < avg else '1' for x in im_data]

def dhash(im, size=(8, 9)):
    pixels = im.resize(size, Image.ANTIALIAS).load()
    bin_list = list()
    h, w = size
    for i in range(h):
        for j in range(w-1):
            b = '0' if pixels[i, j] < pixels[i, j+1] else '1'
            bin_list.append(b)

    return bin_list

def dHashBin(imgfile, size=(8, 9)):
    im = Image.open(imgfile)
    return dhash(im, size)

def pHashBin(imgfile):
    img = cv2.imread(imgfile, 0)
    img = cv2.resize(img, (32, 32), interpolation=cv2.INTER_AREA)
    dct = np.zeros((32, 32), np.float32)
    dct[:32, :32] = img
    dct = cv2.dct(dct)
    dct_8 = dct[:8, :8]
    
    dl = np.array(dct_8.tolist()).flatten()
    avg = dl.mean()
    
    return ['0' if d < avg else '1' for d in dl]

def binlist2int(l):
    return int(''.join(l), 2)

def binlist2hex(l):
    s = len(l)
    return ''.join(['%x' % int(''.join(l[x:x+4]),2) for x in range(0, s, 4)])

def bin_fix_w(n, l=8):
    return bin(n)[2:].rjust(l, '0')

def xor_count(a, b):
    return bin(a^b).count('1')

def show_xor(a, b, l=8):
    print('{}\nxor\n{}\nequal\n{}'.format(bin_fix_w(a, l), bin_fix_w(b, l), bin_fix_w(a^b, l)))

def ahash64(imgfile):
    return binlist2int(aHashBin(imgfile))

def dhash64(imgfile):
    return binlist2int(dHashBin(imgfile))

def phash64(imgfile):
    return binlist2int(pHashBin(imgfile))