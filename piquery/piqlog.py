from config.piq_db import config as db_cfg
from piquery.piqdb import MysqlDb

class PiqLog:
    def __init__(self):
        self.db = MysqlDb(host=db_cfg['host'], user=db_cfg['user'], passwd=db_cfg['passwd'], db=db_cfg['db'])
    def write(self, img_url, _hash, hash_match_num, down_time, db_time, sim_time, total_time, repeat, sim, cid, aid):
        sql = """INSERT INTO case_art_image_distinct_log(img_url,
                    hash, hash_match_num, down_time, db_time, sim_time, total_time, `repeat`, sim, cid, aid)
                    VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(img_url, _hash, hash_match_num, int(down_time*1000), int(db_time*1000), int(sim_time*1000), int(total_time*1000), repeat, sim, cid, aid)
        # print('insert log {}'.format(sql))
        return self.db.write(sql)
