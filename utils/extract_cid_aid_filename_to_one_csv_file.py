import codecs
from time import time
from os import path, makedirs, listdir
import re
import threading

f_tar = 'data/art/mul_thread/{}-cid_aid_filename_8354754.csv'
f_error = 'data/art/mul_thread/{}-ignore_images.csv'

def task(fs_src, thread_id):
    start_time = time()
    for f in fs_src:
        start_time_loop = time()
        extract_cid_aid_filename(f, f_tar.format(thread_id), f_error.format(thread_id))
        print('{} loop cost time {}s\n'.format(thread_id, time() - start_time_loop))
    print('------------\n{} total cost time {}s'.format(thread_id, time() - start_time))

class myThread (threading.Thread):
    def __init__(self, thread_id, fs_src):
        super().__init__()
        self.thread_id = thread_id
        self.fs_src = fs_src
    def run(self):
        print ("start " + self.thread_id)
        task(self.fs_src, self.thread_id)
        print ("exit " + self.thread_id)

def append_line_to_file(f_path, line):
    f_dir = path.dirname(f_path)
    try:
        if not path.isdir(f_dir):
            makedirs(f_dir)
    except FileExistsError:
        pass

    with open(f_path, 'a+', encoding='utf-8') as f:
        f.write('{}\n'.format(line))
        f.close()

def extract_cid_aid_filename(f_src, f_tar, f_error):
    _re = re.compile(r'.(jpg|jpeg|png)$', flags = re.I)
    _is_first_row = True
    with codecs.open(f_src, 'r', encoding='utf-8') as f:
        try:
            for line in f:
                if _is_first_row:
                    _is_first_row = False
                    continue
                try:
                    row = line.strip().split(',')
                    new_line = '{},{},{}'.format(row[1], row[6], row[7]).strip()
                    cid = int(row[1])
                    aid = int(row[6])
                    filename = str(row[7]).strip()
                    if filename and _re.search(filename):
                        append_line_to_file(f_tar, new_line)
                    else:
                        append_line_to_file(f_error, new_line)
                except IndexError as e:
                    append_line_to_file(f_error, line.strip())
                except ValueError as e:
                    append_line_to_file(f_error, new_line)
        except UnicodeEncodeError as e:
             print('UnicodeEncodeError: ', line)
        f.close()

if __name__ == '__main__':
    all_csvs = ['data/deploy/split_data/3/' + f for f in listdir('data/deploy/split_data/3')]
    max_num = 8
    total = len(all_csvs)
    step = int(total / max_num)

    sp = list(range(0, total, step))
    sp[-1] = total

    thread_fs = list()

    for i, d in enumerate(sp):
        if i > 0:
            thread_fs.append(all_csvs[sp[i-1]:d])

    threads = list()

    for i, fs_src in enumerate(thread_fs):
        threads.append(myThread("thread-{}".format(i+1), fs_src))

    for thread in threads:
        thread.start()
    # total_num = 0
    # for fs in thread_fs:
    #     total_num += len(fs)
    #     print(len(fs))
    #     for f in fs:
    #         print(f)
    #     print('------\n')
    # print(total_num)
