from query import Users

class UserBatch():
    def __init__(self, engine, conn):
        self._engine = engine
        self._conn = conn

        self._Users_ins = Users.Users(self._engine, self._conn)

    # 批次新增球員(先檢查重複名單)
    def batch_add_users(self, data={}):
      names = data.names # 批次新增的名稱們
      force = getattr(data, 'force', False) # 是否略過LINE檢查

      repeat_name, repeat_line, add_data = [], [], []
      for name in data.names :
        result = self._Users_ins.get_data({'name-ab':name})
        if len(result.data)>0 : # 姓名完全重複
          repeat_name.append(name)
          continue
        if not force:
          result = self._Users_ins.get_data({'name_line-ab':name})
          if len(result.data)>0 : # LINE名稱完全重複
            repeat_line.append(name)
            continue
        add_data.append({'name':name,'name_line':name,'name_nick':name,})

      if len(add_data)>=0 :
        result = self._Users_ins.insert_data(add_data)

      return {
        **result,
        'repeat_name':repeat_name, 
        'repeat_line':repeat_line,
      }