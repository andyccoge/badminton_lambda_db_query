from query.DBBase import DBBase
from sqlalchemy import Table, MetaData, select, or_, delete, update, insert

class Courts(DBBase):
    _table_name = 'courts'
    _main_col = 'code'
    _court_type = ["", "比賽", "預備"]

    def __init__(self, engine, conn):
        self._engine = engine
        self._conn = conn

        metadata = MetaData()
        self._table = Table(self._table_name, metadata, autoload_with=self._engine)
        self._cols = self._table.c

    # 取得場地
    def get_data(self, where={}):
        # 設定撈取欄位&表
        db_query = select(
                        self._cols.id,
                        self._cols.play_date_id,
                        self._cols.code,
                        self._cols.type
                    ).select_from(self._table)

        # 設定篩選條件
        db_query = self.deal_where_query(db_query, where)
        # 設定排序
        db_query = db_query.order_by(self._cols.id)

        # 執行sql
        result = self._conn.execute(db_query)
        
        # 轉成 list of dict
        rows = [dict(row._mapping) for row in result]

        # 依比賽跟預備把場地分組
        courts_match = [item for item in rows if item.get('type') == 1]
        courts_prepare = [item for item in rows if item.get('type') == 2]

        return {
            "data": rows, "courts_match":courts_match, "courts_prepare":courts_prepare,
            "court_type": self._court_type
        }

    # 刪除場地
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

    # 編輯場地
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

    # 新增場地
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
                case 'play_date_id':
                    db_query = db_query.where(self._cols.play_date_id == value)
                case 'code':
                    db_query = db_query.where(self._cols.code.like(f'%{value}%'))
                case 'type':
                    db_query = db_query.where(self._cols.type == value)
        return db_query

    def check_new_data(self, data):
        error_msgs = []
        # 新增檢查
        if 'play_date_id' not in data: 
            error_msgs.append('請設定對應打球日')
        if 'type' not in data: 
            error_msgs.append('請設定場地類型')
        
        # 一般檢查
        [error_msg2, data] = self.check_data(data)
        if error_msg2: error_msgs.append(error_msg2)
        return ['、'.join(error_msgs), data]

    def check_data(self, data):
        error_msgs = []
        if 'id' in data: del data['id']
        if 'created_at' in data: del data['created_at']
        if 'updated_at' in data: del data['updated_at']

        if 'play_date_id' in data and not data['play_date_id']:
            error_msgs.append('請設定對應打球日')
        if 'type' in data and not data['type'] in [1,2]:
            error_msgs.append('場地類型設定有誤')

        return ['、'.join(error_msgs), data]