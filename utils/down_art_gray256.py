import requests
from PIL import Image
from io import BytesIO
from os import path, makedirs
import codecs
import threading
from time import time

down_rootdir = '/data/kdd/cache/art_gray_256'
failed_file = '{}/failed'.format(down_rootdir)

def task(thread_id):
    start_time = time()
    thread_download(thread_id)
    print('------------\nthread {} cost time {}s'.format(thread_id, time() - start_time))

class myThread (threading.Thread):
    def __init__(self, thread_id):
        super().__init__()
        self.thread_id = thread_id
    def run(self):
        print ("start thread {}".format(self.thread_id))
        task(self.thread_id)
        print ("exit thread {}".format(self.thread_id))

def append_line_to_file(f_path, line):
    try:
        f_dir = path.dirname(f_path)
        if not path.isdir(f_dir):
            makedirs(f_dir)

        with open(f_path, 'a+', encoding='utf-8') as f:
            f.write('{}\n'.format(line))
            f.close()
    except:
        pass

def down_gray_thumbnail(filename, save_path, size=(256, 256)):
    img_url = 'http://pic.to8to.com/case/{}'.format(filename)
    try:
        res = requests.get(img_url)
    except:
        append_line_to_file(failed_file, path.basename(save_path) + ',' + filename)
        return None
    
    if res.status_code != 200:
        print('{} download failed, with status code {}\n'.format(img_url, res.status_code))
        append_line_to_file(failed_file, path.basename(save_path) + ',' + filename)
        return

    try:
      im = Image.open(BytesIO(res.content))
      im.thumbnail(size, Image.ANTIALIAS)
      im.convert('L' ).save(save_path)
    except OSError:
      append_line_to_file(failed_file, path.basename(save_path) + ',' + filename)
    except:
      append_line_to_file(failed_file, path.basename(save_path) + ',' + filename)

    return im

def thread_download(thread_id):
    in_file = 'in/thread-{}-cid_aid_filename.csv'.format(thread_id)
    print('downloading images listed in input file {} ...'.format(in_file))
    with codecs.open(in_file, 'r', encoding='utf-8') as f:
        for line in f:
            cid, aid, filename = line.strip().split(',')
            ext = path.splitext(filename)[1]
            save_path = '{}/{}/{}_{}{}'.format(down_rootdir, cid, cid, aid, ext)
            save_dir = path.dirname(save_path)

            try:
                if not path.isdir(save_dir):
                    makedirs(save_dir)
            except FileExistsError:
                pass

            down_gray_thumbnail(filename, save_path)
        f.close()
    print('{} download completed.'.format(in_file))

if __name__ == '__main__':
    # threads = list()

    # for i in range(1, 9):
    #     threads.append(myThread(i))

    # for thread in threads:
    #     thread.start()

    for i in range(1, 9):
        thread_download(i)