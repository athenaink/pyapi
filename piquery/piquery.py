from abc import ABCMeta, abstractmethod
import json
from piquery.piqimg import ImgDownloader, ImgTransformer, ImgFeature
from piquery.piqdb import MysqlDb
from piquery.piqlog import PiqLog
from time import time
from config.piq_db import config as db_cfg

class Response:
    @staticmethod
    def json(errorcode=0, errormsg='ok', **kwargs):
        rs = dict(errorcode=0, errormsg='ok')
        for k, v in kwargs.items():
            rs[k] = v
        return json.dumps(rs)

class HashQueryStrategy(metaclass = ABCMeta):
    def __init__(self, db):
        self.db = db
    @abstractmethod
    def query(self, _hash):
        pass

class FullHashQueryStrategy(HashQueryStrategy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    def query(self, _hash):
        # 根据图片hash从数据库里查询相同hash的图片指纹
        sql = 'select * from case_art_image_distinct where hash = "{}" and delete_time is null'.format(_hash)
        return self.db.read_sql(sql)

class LimitHashQueryStrategy(HashQueryStrategy):
    def __init__(self, limit=1000, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.limit = limit
    def query(self, _hash):
        # 根据图片hash从数据库里查询相同hash的图片指纹
        sql = 'select * from case_art_image_distinct where hash = "{}" and delete_time is null limit {}'.format(_hash, self.limit)
        return self.db.read_sql(sql)

class CaseHashQueryStrategy(HashQueryStrategy):
    def __init__(self, case_ids=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.case_ids = case_ids
    def query(self, _hash):
        # 根据图片hash从数据库里查询相同hash的图片指纹
        sql = 'select * from case_art_image_distinct where cid in ({}) and hash = "{}" and delete_time is null'.format(self.case_ids, _hash)
        return self.db.read_sql(sql)

class PIQuery:
    def __init__(self, hash_query, img_feature):
        self.hash_query = hash_query
        self.img_feature = img_feature
        self.log = PiqLog()
    def querySim(self, url):
        pass
    def queryRepeat(self, url):
        try:
            start_time = time()
            start_time_ = start_time
            img_rgb = ImgDownloader.download_numpy(url)
            download_time = time() - start_time
            # print('download_time : {}'.format(download_time))

            img_gray = ImgTransformer.bgr2gray(ImgTransformer.rgb2bgr(img_rgb))
            img_hash = self.img_feature.gray2hash(img_gray)
            img_des = self.img_feature.gray2des(img_gray)

            start_time = time()
            hash_df = self.hash_query.query(img_hash)
            db_time = time() - start_time
            # print('db_time : {}'.format(db_time))

            repeat = False
            repeat_confidence = 0.0
            sim_cid = 0
            sim_id = 0

            start_time = time()
            for i in hash_df.index:
                cid, _id, fp = hash_df.loc[i, ['cid', 'id', 'fp']]
                des = self.img_feature.fp2des(fp)
                sim = self.img_feature.sim(img_des, des)
                if sim > 0.01:
                    repeat = True
                    repeat_confidence = sim
                    sim_cid = cid
                    sim_id = _id
                    break
            sim_time = time() - start_time
            # print('sim_time : {}'.format(sim_time))
        except:
            return Response.json(errorcode=500, errormsg='server inner exception!')
        self.log.write(url, img_hash, len(hash_df), download_time, db_time, sim_time, time() - start_time_, 1 if repeat else 0, repeat_confidence, sim_cid, sim_id)
        if repeat:
            return Response.json(repeat=repeat, repeat_confidence=repeat_confidence, cid=int(sim_cid), id=int(sim_id))
        return Response.json(repeat=repeat)

class DbCommand(metaclass = ABCMeta):
    def __init__(self, db):
        self.db = db
    @abstractmethod
    def execute(self):
        pass

class DelDbCommand(DbCommand):
    def __init__(self, cid, _id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cid = cid
        self.id = _id
    def execute(self):
        # 根据图片hash从数据库里查询相同hash的图片指纹
        # sql = 'delete from case_art_image_distinct where cid="{}" and id="{}"'.format(self.cid, self.id)
        sql = 'update case_art_image_distinct set delete_time="{}" where cid="{}" and id="{}"'.format(int(time()), self.cid, self.id)
        return self.db.write(sql)

class AddDbCommand(DbCommand):
    def __init__(self, cid, _id, url, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cid = cid
        self.id = _id
        self.url = url
        self.img_feature = ImgFeature()
    def execute(self):
        img_rgb = ImgDownloader.download_numpy(self.url)
        img_gray = ImgTransformer.bgr2gray(ImgTransformer.rgb2bgr(img_rgb))
        img_hash = self.img_feature.gray2hash(img_gray)
        img_des = self.img_feature.gray2des(img_gray)
        img_fp = self.img_feature.des2fp(img_des)
        # 根据图片hash从数据库里查询相同hash的图片指纹
        sql = 'insert into case_art_image_distinct (`cid`, `id`, `hash`, `fp`, `filename`) values ("{}", "{}", "{}", "{}", "{}")'.format(self.cid, self.id, img_hash, img_fp, self.url)
        return self.db.write(sql)

class PIQBuilder:
    @staticmethod
    def build():
        # print('connect to host {}'.format(db_cfg['host']))
        db = MysqlDb(host=db_cfg['host'], user=db_cfg['user'], passwd=db_cfg['passwd'], db=db_cfg['db'])
        limit_hash_query = FullHashQueryStrategy(db=db)

        img_feature = ImgFeature()

        return PIQuery(limit_hash_query, img_feature)
    @staticmethod
    def buildCase(case_ids):
        # print('connect to host {}'.format(db_cfg['host']))
        db = MysqlDb(host=db_cfg['host'], user=db_cfg['user'], passwd=db_cfg['passwd'], db=db_cfg['db'])
        limit_hash_query = CaseHashQueryStrategy(db=db, case_ids=case_ids)

        img_feature = ImgFeature()

        return PIQuery(limit_hash_query, img_feature)
    @staticmethod
    def buildAddCmd(cid, _id, url):
        db = MysqlDb(host=db_cfg['host'], user=db_cfg['user'], passwd=db_cfg['passwd'], db=db_cfg['db'])
        return AddDbCommand(db=db, cid=cid, _id=_id, url=url)
    @staticmethod
    def buildDelCmd(cid, _id):
        db = MysqlDb(host=db_cfg['host'], user=db_cfg['user'], passwd=db_cfg['passwd'], db=db_cfg['db'])
        return DelDbCommand(db=db, cid=cid, _id=_id)