from abc import ABC, abstractmethod

class DBBase(ABC):
    _engine=None
    _conn=None

    #初始化連線
    @abstractmethod
    def __init__(self, engine, conn):
        pass

    # 取得
    @abstractmethod
    def get_data(self, where={}):
        pass

    # 刪除
    @abstractmethod
    def delete_data(self, where={}):
        pass

    # 編輯
    @abstractmethod
    def update_data(self, where, data={}):
        pass

    # 新增
    @abstractmethod
    def insert_data(self, data={}):
        pass


    # 處理篩選條件
    @abstractmethod
    def deal_where_query(self, db_query, where):
        pass