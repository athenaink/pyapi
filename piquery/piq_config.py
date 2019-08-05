import os

hostname = os.popen('hostname').read().strip()
if "dmp" in hostname:
    dbhost = '10.10.10.243'
    dbuser = 'dmp_admin'
    dbpasswd = 'fnWNJPCiGRje16pV'
    dbname = 'dmp_uniq'
else:
    dbhost = '192.168.11.112'
    dbuser = 'to8to'
    dbpasswd = 'to8to123'
    dbname = 't8t-bi-report'

config = {
  'host': dbhost,
  'user': dbuser,
  'passwd': dbpasswd,
  'db': dbname,
  'table': 'case_art_image_distinct_test', # case_art_image_distinct
  'table_hash': 'case_art_image_hash', # image dhash
  'table_log': 'case_art_image_distinct_log_test', # case_art_image_distinct_log
  'table_cache': 'case_art_image_distinct_cache', # redis缓存记录表
  'cache_dir': '/data/kdd/piq_cache/',
  'redis_host': dbhost,
  'redis_port': 6379
}
