from piquery.piq_hash64 import download_to_hash
from piquery.piq_error import DownloadError, ImageFormatError
from piquery.piq_config import config as db_cfg
from piquery.piq_response import Response

class PIQueryHash64:
    def __init__(self, db):
        self.db = db
    def queryRepeat(self, url):
        try:
            img_hash = download_to_hash(url)
            sql = 'select cid, aid from {} where `dhash`="{}" limit 1'.format(db_cfg['table_hash'], img_hash)
            db_row = self.db.read(sql)
        except DownloadError as err:
            return Response.json(errorcode=501, errormsg=repr(err))
        except ImageFormatError as err:
            return Response.json(errorcode=502, errormsg=repr(err))
        except:
            return Response.json(errorcode=500, errormsg='server inner exception!')

        if db_row:
            return Response.json(repeat=True, repeat_confidence=1.0, cid=db_row[0], id=db_row[1])

        return Response.json(repeat=False)

class AddHashCommand:
    def __init__(self, db, cid, _id, url):
        self.db = db
        self.cid = cid
        self.id = _id
        self.url = url
    def _exist(self):
        return self.db.read('select * from {} where `cid`="{}" and `aid`="{}"'.format(db_cfg['table_hash'], self.cid, self.id))
    def execute(self):
        if self._exist():
            return True

        img_hash = img_hash = download_to_hash(self.url)

        sql = 'insert into {} (`url`, `cid`, `aid`, `dhash`) values ("{}", "{}", "{}", "{}")'.format(db_cfg['table_hash'], self.url, self.cid, self.id, img_hash)
        return self.db.write(sql)

class DelHashCommand:
    def __init__(self, db, cid, _id):
        self.db = db
        self.cid = cid
        self.id = _id
    def _exist(self):
        return self.db.read('select * from {} where `cid`="{}" and `aid` in ({})'.format(db_cfg['table_hash'], self.cid, self.id))
    def execute(self):
        if not self._exist():
            return True

        sql = 'delete from {} where cid="{}" and `aid` in ({})'.format(db_cfg['table_hash'], self.cid, self.id)
        return self.db.write(sql)
