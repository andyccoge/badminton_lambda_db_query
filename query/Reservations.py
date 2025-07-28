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
        db_query = self.deal_where_query(db_query, where)
        # 設定排序
        db_query = db_query.order_by(self._cols.id)

        # 執行sql
        result = self._conn.execute(db_query)
        
        # 轉成 list of dict
        rows = [dict(row._mapping) for row in result]

        return {"data": rows}

    # 刪除報名紀錄
    def delete_data(self, where={}):
        if where=={}: return 0

        # 設定刪除表
        db_query = delete(self._table)

        # 設定篩選條件
        db_query = self.deal_where_query(db_query, where)

        # 執行sql
        result = self._conn.execute(db_query)
        self._conn.commit()

        return result.rowcount

    # 編輯報名紀錄
    def update_data(self, where, data={}):
        msg = ''
        [error_msg, data] = self.check_data(data)
        if error_msg: msg += self.set_error_msg(data.get(self._main_col, ''), error_msg)

        # 檢查有誤
        if msg:
            return {'saved':0, 'msg':msg}
        
        db_query = update(self._table)
        db_query = db_query.values(**data)

        # 設定篩選條件
        db_query = self.deal_where_query(db_query, where)

        result = self._conn.execute(db_query)
        self._conn.commit()

        return result.rowcount        

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
        stmt = select(self._table).order_by(self._cols.id.desc()).limit(len(data))
        rows = self._conn.execute(stmt).fetchall()

        # 取得主鍵 id 清單（記得反轉順序）
        pk_list = [row._mapping['id'] for row in reversed(rows)]
        return {'saved':pk_list, 'msg':''}


    def deal_where_query(self, db_query, where):
        for key, value in where.items():
            if value=='' or value is None:
                continue
            match key:
                case 'id':
                    db_query = db_query.where(self._cols.id == value)
                case 'user_id':
                    db_query = db_query.where(self._cols.user_id == value)
                case 'play_date_id':
                    db_query = db_query.where(self._cols.play_date_id == value)
                case 'show_up':
                    db_query = db_query.where(self._cols.show_up == value)
                case 'leave':
                    db_query = db_query.where(self._cols.leave == value)
                case 'paid':
                    db_query = db_query.where(self._cols.paid == value)
                # 以下是join球員表的篩選
                case 'email':
                    db_query = db_query.where(self._users.c.email.like(f'%{value}%'))
                case 'cellphone':
                    db_query = db_query.where(self._users.c.cellphone.like(f'%{value}%'))
                case 'gender':
                    db_query = db_query.where(self._users.c.gender == value)
                case 'name_keyword':
                    db_query = db_query.where(
                        or_(
                            self._users.c.name.like(f'%{value}%'),
                            self._users.c.name_line.like(f'%{value}%'),
                            self._users.c.name_nick.like(f'%{value}%')
                        )
                    )
                case 'level_over':
                    db_query = db_query.where(self._users.c.level >= value)
        return db_query

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