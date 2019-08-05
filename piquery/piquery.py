from abc import ABCMeta, abstractmethod
from piquery.piqimg import ImgDownloader, ImgTransformer, ImgFeature
from piquery.piqdb import MysqlDb
from piquery.piqlog import PiqLog
from time import time
from piquery.piq_config import config as db_cfg
from piquery.piq_error import DownloadError, ImageFormatError
from piquery.piq_cache import saveCache, loadCache, RedisCache
from piquery.piq_hash64_query import PIQueryHash64, AddHashCommand, DelHashCommand
from os import path
import hashlib
from piquery.piq_response import Response

def md5(s):
    if isinstance(s, str):
        return hashlib.md5(s.encode(encoding='utf-8')).hexdigest()
    else:
        try:
            s = str(s)
        except:
            return None
        return hashlib.md5(s.encode(encoding='utf-8')).hexdigest()

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
        sql = 'select * from {} where hash = "{}"'.format(db_cfg['table'], _hash)
        return self.db.read_sql(sql)

class LimitHashQueryStrategy(HashQueryStrategy):
    def __init__(self, limit=1000, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.limit = limit
    def query(self, _hash):
        # 根据图片hash从数据库里查询相同hash的图片指纹
        sql = 'select * from {} where hash = "{}" limit {}'.format(db_cfg['table'], _hash, self.limit)
        return self.db.read_sql(sql)

class CaseHashQueryStrategy(HashQueryStrategy):
    def __init__(self, case_ids=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.case_ids = case_ids
    def query(self, _hash):
        # 根据图片hash从数据库里查询相同hash的图片指纹
        sql = 'select * from {} where cid in ({}) and hash = "{}"'.format(db_cfg['table'], self.case_ids, _hash)
        return self.db.read_sql(sql)

class PIQueryProxy:
    def __init__(self, piq):
        self.piq = piq
        self.cache = RedisCache()
    def saveCacheRecordToDb(self, md, _hash, repeat, sim, cid, aid):
        sql = 'insert into {} (`md`, `hash`, `repeat`, `sim`, `cid`, `aid`) values ("{}", "{}", "{}", "{}", "{}", "{}")'.format(db_cfg['table_cache'], md, _hash, repeat, sim, cid, aid)
        return self.piq.hash_query.db.write(sql)
    def preprocHash(self, img_hash):
        piq = self.piq
        try:
            if not path.isdir(db_cfg['cache_dir'] + img_hash):
                hash_df = piq.hash_query.query(img_hash)
                saveCache(img_hash, hash_df, lambda x:piq.img_feature.fp2des(x))
        except:
            return Response.json(errorcode=500, errormsg='server inner exception!')
        return Response.json()
    def queryRepeat(self, url):
        piq = self.piq
        try:
            img_hash, img_des = piq.getHashAndDes(url)
            img_fp = piq.img_feature.des2fp(img_des)
            cache_key = md5(img_fp)
            cache_val = self.cache.get(cache_key)
            if cache_val is not None:
                sim_cid, sim_id, repeat_confidence = cache_val.split('_')
                return Response.json(repeat=True, repeat_confidence=float(repeat_confidence), cid=int(sim_cid), id=int(sim_id))

            if not path.isdir(db_cfg['cache_dir'] + img_hash):
                hash_df = piq.hash_query.query(img_hash)
                saveCache(img_hash, hash_df, lambda x:piq.img_feature.fp2des(x))

            repeat, repeat_confidence, sim_cid, sim_id = piq.quickQueryRepeat(img_hash, img_des)
        except DownloadError as err:
            return Response.json(errorcode=501, errormsg=repr(err))
        except ImageFormatError as err:
            return Response.json(errorcode=502, errormsg=repr(err))
        except:
            return Response.json(errorcode=500, errormsg='server inner exception!')

        if repeat:
            self.cache.write(cache_key, "{}_{}_{}".format(sim_cid, sim_id, repeat_confidence))
            self.saveCacheRecordToDb(cache_key, img_hash, 1, repeat_confidence, sim_cid, sim_id)
            return Response.json(repeat=repeat, repeat_confidence=repeat_confidence, cid=int(sim_cid), id=int(sim_id))
        return Response.json(repeat=False)

class PIQuery:
    def __init__(self, hash_query, img_feature):
        self.hash_query = hash_query
        self.img_feature = img_feature
        self.log = PiqLog()
    def getHashAndDes(self, url):
        img_rgb = ImgDownloader.download_numpy(url)
        img_gray = ImgTransformer.bgr2gray(ImgTransformer.rgb2bgr(img_rgb))
        img_hash = self.img_feature.gray2hash(img_gray)
        img_des = self.img_feature.gray2des(img_gray)
        return img_hash, img_des
    def quickQueryRepeat(self, img_hash, img_des):
        repeat = False
        repeat_confidence = 0.0
        sim_cid = 0
        sim_id = 0
        desLoader = loadCache(img_hash)
        for (cid, _id), des in desLoader:
            sim = self.img_feature.sim(img_des, des)
            if sim > 0.01:
                repeat = True
                repeat_confidence = sim
                sim_cid = cid
                sim_id = _id
                break
        return repeat, repeat_confidence, sim_cid, sim_id
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
        except DownloadError as err:
            return Response.json(errorcode=501, errormsg=repr(err))
        except ImageFormatError as err:
            return Response.json(errorcode=502, errormsg=repr(err))
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
    def _exist(self):
        return bool(self.db.read('select * from {} where `cid`="{}" and `id` in ({})'.format(db_cfg['table'], self.cid, self.id)))
    def execute(self):
        if not self._exist():
            return True
        # 根据图片hash从数据库里查询相同hash的图片指纹
        sql = 'delete from {} where cid="{}" and `id` in ({})'.format(db_cfg['table'], self.cid, self.id)
        # sql = 'update {} set delete_time="{}" where `cid`="{}" and `id` in ({})'.format(db_cfg['table'], int(time()), self.cid, self.id)
        return self.db.write(sql)

class AddDbCommand(DbCommand):
    def __init__(self, cid, _id, url, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cid = cid
        self.id = _id
        self.url = url
        self.img_feature = ImgFeature()
    def _exist(self):
        return bool(self.db.read('select * from {} where `cid`="{}" and `id`="{}"'.format(db_cfg['table'], self.cid, self.id)))
    def _cancel_del(self):
        sql = 'update {} set delete_time=NULL where cid="{}" and `id`="{}"'.format(db_cfg['table'], self.cid, self.id)
        self.db.write(sql)
    def execute(self):
        if self._exist():
            self._cancel_del()
            return True

        img_rgb = ImgDownloader.download_numpy(self.url)

        img_gray = ImgTransformer.bgr2gray(ImgTransformer.rgb2bgr(img_rgb))
        img_hash = self.img_feature.gray2hash(img_gray)
        img_des = self.img_feature.gray2des(img_gray)
        img_fp = self.img_feature.des2fp(img_des)
        # 根据图片hash从数据库里查询相同hash的图片指纹
        sql = 'insert into {} (`cid`, `id`, `hash`, `fp`, `filename`) values ("{}", "{}", "{}", "{}", "{}")'.format(db_cfg['table'], self.cid, self.id, img_hash, img_fp, self.url)
        return self.db.write(sql)

class PIQBuilder:
    @staticmethod
    def buildHashQuery():
        db = MysqlDb(host=db_cfg['host'], user=db_cfg['user'], passwd=db_cfg['passwd'], db=db_cfg['db'])

        return PIQueryHash64(db)
    @staticmethod
    def build():
        # print('connect to host {}'.format(db_cfg['host']))
        db = MysqlDb(host=db_cfg['host'], user=db_cfg['user'], passwd=db_cfg['passwd'], db=db_cfg['db'])
        limit_hash_query = FullHashQueryStrategy(db=db)

        img_feature = ImgFeature()

        return PIQueryProxy(PIQuery(limit_hash_query, img_feature))
    @staticmethod
    def buildCase(case_ids):
        # print('connect to host {}'.format(db_cfg['host']))
        db = MysqlDb(host=db_cfg['host'], user=db_cfg['user'], passwd=db_cfg['passwd'], db=db_cfg['db'])
        limit_hash_query = CaseHashQueryStrategy(db=db, case_ids=case_ids)

        img_feature = ImgFeature()

        return PIQuery(limit_hash_query, img_feature)
    # @staticmethod
    # def buildAddCmd(cid, _id, url):
    #     db = MysqlDb(host=db_cfg['host'], user=db_cfg['user'], passwd=db_cfg['passwd'], db=db_cfg['db'])
    #     return AddDbCommand(db=db, cid=cid, _id=_id, url=url)
    # @staticmethod
    # def buildDelCmd(cid, _id):
    #     db = MysqlDb(host=db_cfg['host'], user=db_cfg['user'], passwd=db_cfg['passwd'], db=db_cfg['db'])
    #     return DelDbCommand(db=db, cid=cid, _id=_id)
    @staticmethod
    def buildAddCmd(cid, _id, url):
        db = MysqlDb(host=db_cfg['host'], user=db_cfg['user'], passwd=db_cfg['passwd'], db=db_cfg['db'])
        return AddHashCommand(db=db, cid=cid, _id=_id, url=url)
    @staticmethod
    def buildDelCmd(cid, _id):
        db = MysqlDb(host=db_cfg['host'], user=db_cfg['user'], passwd=db_cfg['passwd'], db=db_cfg['db'])
        return DelHashCommand(db=db, cid=cid, _id=_id)
