from query import Users, PlayDate, Courts, Reservations, Matchs

class PlayDateData():
  def __init__(self, engine, conn):
    self._engine = engine
    self._conn = conn

    self._Users_ins = Users.Users(self._engine, self._conn)
    self._PlayDate_ins = PlayDate.PlayDate(self._engine, self._conn)
    self._Courts_ins = Courts.Courts(self._engine, self._conn)
    self._Reservations_ins = Reservations.Reservations(self._engine, self._conn)
    self._Matchs_ins = Matchs.Matchs(self._engine, self._conn)

  # 取得打球日所需全部資料
  def get_data(self, where={}):
    if 'id' not in where: return {'msg':'打球日有誤'}

    # 打球日資料
    play_date = self._PlayDate_ins.get_data(where)["data"]
    if len(play_date)==0: return {'msg':'無此打球日'}
    play_date = play_date[0]

    # 場地資料
    courts_r = self._Courts_ins.get_data({'play_date_id':play_date['id']})
    courts = courts_r["data"]
    courts_match = courts_r["courts_match"]
    courts_prepare = courts_r["courts_prepare"]
    court_type = courts_r["court_type"]

    # 報名紀錄
    reservations = self._Reservations_ins.get_data({'play_date_id':play_date['id']})["data"]
    all_user_map = {reservation['user_id']:{
      'id':reservation['user_id'],
      'name':reservation['name'],
      'name_line':reservation['name_line'],
      'name_nick':reservation['name_nick'],
      'level':reservation['level'],
      'gender':reservation['gender'],
      'email':reservation['email'],
      'cellphone':reservation['cellphone'],
    } for reservation in reservations }

    # 比賽紀錄
    matchs_result = self._Matchs_ins.get_data({'play_date_id':play_date['id']})
    matchs = matchs_result["data"]
    user_map = matchs_result["user_map"]

    all_user_map.update(user_map)

    return {
      'msg':'', 
      'play_date':play_date,
      'courts':courts,
      'courts_match':courts_match,
      'courts_prepare':courts_prepare,
      'court_type':court_type,
      'reservations':reservations,
      'matchs':matchs,
      'user_map':all_user_map,
    }