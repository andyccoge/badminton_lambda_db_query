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

        # 比賽紀錄
        matchs = self._Matchs_ins.get_data({'play_date_id':play_date['id']})["data"]

        # 報名紀錄&比賽紀錄相關人員(以id為key)
        user_ids = [item['user_id'] for item in reservations]
        for item in matchs:
            user_ids.append(item['user_id_1'])
            user_ids.append(item['user_id_2'])
            user_ids.append(item['user_id_3'])
            user_ids.append(item['user_id_4'])
        users = self._Users_ins.get_data({'ids':user_ids})["data"]
        user_map = {item['id']:item for item in users }

        return {
            'msg':'', 
            'play_date':play_date,
            'courts':courts,
            'courts_match':courts_match,
            'courts_prepare':courts_prepare,
            'court_type':court_type,
            'reservations':reservations,
            'matchs':matchs,
            'user_map':user_map,
        }