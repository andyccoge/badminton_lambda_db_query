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
    # 添加sql語句到回傳資料(檢查用)
    # compiled = db_query.compile(compile_kwargs={"literal_binds": True})
    # rows.append(str(compiled))
    pass

  # 刪除
  @abstractmethod
  def delete_data(self, where={}):
    # return {'deleted':result.rowcount, 'msg':''}
    pass

  # 編輯
  @abstractmethod
  def update_data(self, where, data={}):
    # return {'saved':result.rowcount, 'msg':''}
    pass

  # 新增
  @abstractmethod
  def insert_data(self, data={}):
    # return {'saved':pk_list, 'msg':''}
    pass


  # 處理篩選條件
  @abstractmethod
  def deal_where_query(self, db_query, where):
    # return [db_query, filtered] # 回傳組織篩選條件的db_query、where參數是否有套用到篩選
    pass

  # 判斷新資料是否符合新增條件
  @abstractmethod
  def check_new_data(self, data):
    pass

  # 判斷資料格式是否符合資料表要求
  @abstractmethod
  def check_data(self, data):
    pass

  # 依參數組織回傳的錯誤訊息(主要辨別文字[ex:名稱]+錯誤說明)
  def set_error_msg(self, main_str ,error_msg):
    return ((main_str+'：') if main_str.strip() else '') + error_msg + "; "
