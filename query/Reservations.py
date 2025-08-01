from query.DBBase import DBBase
from sqlalchemy import Table, MetaData, select, or_, delete, update, insert

class Reservations(DBBase):
    _table_name = 'reservations'
    _main_col = 'user_id'

    def __init__(self, engine, conn):
        self._engine = engine
        self._conn = conn

        metadata = MetaData()
        self._table = Table(self._table_name, metadata, autoload_with=self._engine)
        self._cols = self._table.c

        self._users = Table('users', metadata, autoload_with=self._engine)

    # 取得報名紀錄
    def get_data(self, where={}):
        # 設定撈取欄位&表
        db_query = select(
                        self._cols.id,
                        self._cols.user_id,
                        self._cols.play_date_id,
                        self._cols.show_up,
                        self._cols.leave,
                        self._cols.paid,

                        self._users.c.name,
                        self._users.c.name_line,
                        self._users.c.name_nick,
                        self._users.c.email,
                        self._users.c.cellphone,
                        self._users.c.gender,
                        self._users.c.level
                    ).select_from(
                        self._table.join(self._users, self._users.c.id == self._cols.user_id)
                    )

        # 設定篩選條件
        [db_query, *_] = self.deal_where_query(db_query, where)
        # 設定排序
        db_query = db_query.order_by(self._cols.id)

        # 執行sql
        result = self._conn.execute(db_query)
        
        # 轉成 list of dict
        rows = [dict(row._mapping) for row in result]

        return {"data": rows}

    # 刪除報名紀錄
    def delete_data(self, where={}):
        # 設定刪除表
        db_query = delete(self._table)

        # 設定篩選條件
        [db_query, filtered] = self.deal_where_query(db_query, where)
        if filtered==0: return {'deleted':0, 'msg':'請設定篩選條件'}

        # 執行sql
        result = self._conn.execute(db_query)
        self._conn.commit()

        return {'deleted':result.rowcount, 'msg':''}

    # 編輯報名紀錄
    def update_data(self, where, data={}):
        msg = ''
        [error_msg, data] = self.check_data(data)
        if error_msg: msg += self.set_error_msg(data.get(self._main_col, ''), error_msg)

        # 檢查有誤
        if msg: return {'saved':0, 'msg':msg}
        
        db_query = update(self._table)
        db_query = db_query.values(**data)

        # 設定篩選條件
        [db_query, filtered] = self.deal_where_query(db_query, where)
        if filtered==0: return {'saved':0, 'msg':'請設定篩選條件'}

        result = self._conn.execute(db_query)
        self._conn.commit()

        return {'saved':result.rowcount, 'msg':msg}

    # 新增報名紀錄
    def insert_data(self, data={}):
        msg = ''
        items = []
        if type(data) is list: # 批次新增
            for item in data:
                [error_msg, item] = self.check_new_data(item)
                if error_msg: msg += self.set_error_msg(item.get(self._main_col, ''), error_msg)
                items.append(item)
        else: # 單個新增
            [error_msg, data] = self.check_new_data(data)
            if error_msg: msg += self.set_error_msg(data.get(self._main_col, ''), error_msg)
            items.append(data)
        
        # 檢查有誤
        if msg:
            return {'saved':0, 'msg':msg}

        db_query = insert(self._table)
        result = self._conn.execute(db_query, items)
        self._conn.commit()
        
        # 再查詢最後 N 筆資料(可考慮 result.rowcount)
        stmt = select(self._table).order_by(self._cols.id.desc()).limit(len(items))
        rows = self._conn.execute(stmt).fetchall()

        # 取得主鍵 id 清單（記得反轉順序）
        pk_list = [row._mapping['id'] for row in reversed(rows)]
        return {'saved':pk_list, 'msg':''}


    def deal_where_query(self, db_query, where):
        filtered = 0
        for key, value in where.items():
            if value=='' or value is None:
                continue
            match key:
                case 'id':
                    db_query = db_query.where(self._cols.id == value)
                    filtered = 1
                case 'ids':
                    db_query = db_query.where(self._cols.id.in_(value))
                    filtered = 1
                case 'user_id':
                    db_query = db_query.where(self._cols.user_id == value)
                    filtered = 1
                case 'play_date_id':
                    db_query = db_query.where(self._cols.play_date_id == value)
                    filtered = 1
                case 'show_up':
                    db_query = db_query.where(self._cols.show_up == value)
                    filtered = 1
                case 'leave':
                    db_query = db_query.where(self._cols.leave == value)
                    filtered = 1
                case 'paid':
                    db_query = db_query.where(self._cols.paid == value)
                    filtered = 1
                # 以下是join球員表的篩選
                case 'email':
                    db_query = db_query.where(self._users.c.email.like(f'%{value}%'))
                    filtered = 1
                case 'cellphone':
                    db_query = db_query.where(self._users.c.cellphone.like(f'%{value}%'))
                    filtered = 1
                case 'gender':
                    db_query = db_query.where(self._users.c.gender == value)
                    filtered = 1
                case 'name_keyword':
                    db_query = db_query.where(
                        or_(
                            self._users.c.name.like(f'%{value}%'),
                            self._users.c.name_line.like(f'%{value}%'),
                            self._users.c.name_nick.like(f'%{value}%')
                        )
                    )
                    filtered = 1
                case 'level_over':
                    db_query = db_query.where(self._users.c.level >= value)
                    filtered = 1
        return [db_query, filtered]

    def check_new_data(self, data):
        error_msgs = []
        # 新增檢查
        if 'user_id' not in data: 
            error_msgs.append('請設定對應球員')
        if 'play_date_id' not in data: 
            error_msgs.append('請設定對應打球日')
        
        # 一般檢查
        [error_msg2, data] = self.check_data(data)
        if error_msg2: error_msgs.append(error_msg2)
        return ['、'.join(error_msgs), data]

    def check_data(self, data):
        error_msgs = []
        if 'id' in data: del data['id']
        if 'created_at' in data: del data['created_at']
        if 'updated_at' in data: del data['updated_at']

        if 'user_id' in data and not data['user_id']:
            error_msgs.append('請設定對應球員')
        if 'play_date_id' in data and not data['play_date_id']:
            error_msgs.append('請設定對應打球日')

        return ['、'.join(error_msgs), data]