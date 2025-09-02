from query import Users, Reservations

class UserBatch():
  def __init__(self, engine, conn):
    self._engine = engine
    self._conn = conn

    self._Users_ins = Users.Users(self._engine, self._conn)
    self._Reservations_ins = Reservations.Reservations(engine, conn)

  # 批次新增球員(先檢查重複名單)
  def batch_add_users(self, data={}):
    names = data.get('names') # 批次新增的名稱們
    force = data.get('force') # 是否略過LINE檢查

    repeat_name, repeat_line, ok_names, new_users = [], [], [], []
    for name in names :
      result = self._Users_ins.get_data({'name-ab':name})
      if len(result['data'])>0 : # 姓名完全重複
        repeat_name.append(name)
        continue
      if force!='force':
        result = self._Users_ins.get_data({'name_line-ab':name})
        if len(result['data'])>0 : # LINE名稱完全重複
          repeat_line.append(name)
          continue
      ok_names.append(name)
      new_users.append({'name':name,'name_line':name,'name_nick':name,'gender':1,})

    if len(repeat_name)==0 and len(repeat_line)==0:
      if len(new_users)>0 :
        result = self._Users_ins.insert_data(new_users)

    return {
      'force':force,
      'repeat_name':repeat_name, 
      'repeat_line':repeat_line,
      'ok_names':ok_names,
    }
  
  def batch_set_reservations(self, data={}):
    names = data.get('names') # 批次新增的名稱們
    play_date_id = data.get('play_date_id') # 新增報名紀錄的打球日

    genter_dict = {
      value: index for index, value in enumerate(self._Users_ins._genter_text)
    }

    repeat_name, fuzzy_names, ok_names, new_users, new_reservations = [], [], [], [], []
    for name_ori in names :
      # 處理名單的篩選條件
      name_s = name_ori.split(':')
      name = name_s[0]
      user_where = {'name_line-ab':name, 'play_date_id':play_date_id,}
      if len(name_s)>1:
        others = name_s[1].split('[')
        if len(others)==2:
          parms = others[1].split(']')[0].split(',')
          if len(parms)==3:
            user_where['name-ab'] = parms[0]
            user_where['name_nick-ab'] = parms[1]
            user_where['gender'] = genter_dict[parms[2]]

      # 判斷會員是否有重複
      result = self._Users_ins.get_data(user_where)
      if len(result['data'])>1 : # 球員中超過1個LINE名稱完全重複
        repeat_data = ''.join([ 
          '['+ item['name'] + ','+ 
              item['name_nick'] + ',' + 
              self._Users_ins._genter_text[item['gender']]
          +']' for item in result['data']
        ])
        fuzzy_names.append(name+':'+repeat_data)
      else:
        result2 = self._Reservations_ins.get_data(user_where)
        if len(result2['data'])>0 : # 姓名完全重複
          repeat_name.append(name_ori)
          continue

        ok_names.append(name_ori)
        if len(result['data'])==0:  # 球員中沒有LINE名稱完全重複
          # 建立新會員
          new_users.append({'name':name,'name_line':name,'name_nick':name,'gender':1,})
        else:
          new_reservations.append({'user_id':result['data'][0]['id'],'play_date_id':play_date_id,})

    if len(repeat_name)==0 and len(fuzzy_names)==0:
      if len(new_users)>0 :
        result = self._Users_ins.insert_data(new_users)
        for id in result['saved']:
          new_reservations.append({'user_id':id,'play_date_id':play_date_id,})
      if len(new_reservations)>0 :
        result = self._Reservations_ins.insert_data(new_reservations)

    return {
      'repeat_name':repeat_name, 
      'fuzzy_names':fuzzy_names,
      'ok_names':ok_names,
    }