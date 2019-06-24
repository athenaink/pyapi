import pymysql
import pandas as pd
from abc import ABCMeta, abstractmethod

class MetaSingleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(MetaSingleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Mysql(metaclass = MetaSingleton):
    connection = None
    def __init__(self, *args, **kwargs):
        self.connect(*args, **kwargs)
    def __call__(self):
        return self.connection
    def connect(self, host, user, passwd, db, port=3306):
        if self.connection is None:
            self.connection = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset='utf8')
            self.cursor = self.connection.cursor()
        return self.cursor
    def __del__(self):
        if self.connection is not None:
            self.connection.close()
    def execute(self, sql):
        self.cursor.execute(sql)
    def fetchone(self, sql):
        self.execute(sql)
        return self.cursor.fetchone()
    def version(self):
        return self.fetchone("SELECT VERSION()")
    def insert(self, sql):
        try:
            self.execute(sql)
            self.connection.commit()
        except:
            # 如果发生错误则回滚
            self.connection.rollback()
    def fetchall(self, sql):
        self.execute(sql)
        return self.cursor.fetchall()

class Db(metaclass = ABCMeta):
    @abstractmethod
    def write(self):
        pass
    @abstractmethod
    def read_sql(self):
        pass

class MysqlDb(Db):
    cursor = None
    def __init__(self, *args, **kwargs):
        self.connection = Mysql(*args, **kwargs)()
    def write(self, sql):
        if self.cursor is None:
            self.cursor = self.connection.cursor()
        try:
            self.cursor.execute(sql)
            self.connection.commit()
        except:
            # 如果发生错误则回滚
            self.connection.rollback()
            return False
        return True
    def read_sql(self, sql):
        return pd.read_sql(sql, self.connection)