import threading
import time
from download_xiaoguotu import download_img_by_filename
import pandas as pd
import math

def download_img(df, thread_id):
    for filename in df['filename']:
        download_img_by_filename(filename, thread_id)
        # time.sleep(1)

class myThread (threading.Thread):
    def __init__(self, thread_id, df):
        super().__init__()
        self.thread_id = thread_id
        self.df = df
    def run(self):
        print ("start " + self.thread_id)
        download_img(self.df, self.thread_id)
        print ("exit " + self.thread_id)


if __name__ == '__main__':
    csv_f = pd.read_csv('ods_to8to_to8to_to8to_xiaoguotu.csv', names=['cid', 'id', 'filename'], header=None)
    max_num = 16
    total = csv_f.shape[0]
    step = math.floor(total / max_num)

    sp = list(range(0, total, step))
    sp[-1] = total

    thread_dfs = list()

    for i, d in enumerate(sp):
        if i > 0:
            thread_dfs.append(csv_f.loc[sp[i-1]:d-1])

    threads = list()

    for i, df in enumerate(thread_dfs):
        threads.append(myThread("thread-{}".format(i+1), df))

    for thread in threads:
        thread.start()