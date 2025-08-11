import json
from query.DBBase import DBBase
from sqlalchemy import Table, MetaData, select, or_, delete, update, insert, desc, func
from datetime import datetime, timedelta

class PlayDate(DBBase):
    _table_name = 'play_date'
    _main_col = 'datetime'

    def __init__(self, engine, conn):
        self._engine = engine
        self._conn = conn

        metadata = MetaData()
        self._table = Table(self._table_name, metadata, autoload_with=self._engine)
        self._cols = self._table.c

    # 取得打球日
    def get_data(self, where={}):
        # 設定撈取欄位&表
        db_query = select(
                        self._cols.id,
                        self._cols.location,
                        self._cols.note,
                        func.date_format(self._cols.datetime, "%Y-%m-%d %H:%i").label("datetime"),
                        func.date_format(self._cols.datetime2, "%Y-%m-%d %H:%i").label("datetime2")
                    ).select_from(self._table)

        # 設定篩選條件
        [db_query, *_] = self.deal_where_query(db_query, where)
        # 設定排序
        db_query = db_query.order_by(desc(self._cols.datetime))

        # 執行sql
        result = self._conn.execute(db_query)
        
        # 轉成 list of dict
        rows = [dict(row._mapping) for row in result]

        return {"data": rows}

    # 刪除打球日
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

    # 編輯打球日
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

    # 新增打球日
    def insert_data(self, data={}):
        msg = ''
        items = []
        if type(data) is list: # 批次新增
            for item in data:
                [error_msg, item] = self.check_new_data(item)
                if error_msg: 
                    msg += self.set_error_msg(item.get(self._main_col, ''), error_msg)
                else:
                    item['datetime'] = datetime.strptime(item.get('datetime', ''), "%Y-%m-%d %H:%M")
                    item['datetime2'] = datetime.strptime(item.get('datetime2', ''), "%Y-%m-%d %H:%M")
                    items.append(item)
        else: # 單個新增
            [error_msg, data] = self.check_new_data(data)
            if error_msg: 
                msg += self.set_error_msg(data.get(self._main_col, ''), error_msg)
            else:
                data['datetime'] = datetime.strptime(data.get('datetime', ''), "%Y-%m-%d %H:%M")
                data['datetime2'] = datetime.strptime(data.get('datetime2', ''), "%Y-%m-%d %H:%M")
                items.append(data)
        
        # 檢查有誤
        if msg: return {'saved':0, 'msg':msg}

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
                    try:
                        if isinstance(value, str): value = json.loads(value)
                    except Exception as e:
                        value = [-1]
                    if value:
                        db_query = db_query.where(self._cols.id.in_(value))
                        filtered = 1
                case 'location':
                    db_query = db_query.where(self._cols.location.like(f'%{value}%'))
                    filtered = 1
                case 'note':
                    db_query = db_query.where(self._cols.note.like(f'%{value}%'))
                    filtered = 1
                case 'datetime_s':
                    start_time = datetime.fromisoformat(value)
                    db_query = db_query.where(self._cols.datetime >= start_time)
                    filtered = 1
                case 'datetime_e':
                    end_time = datetime.fromisoformat(value)
                    db_query = db_query.where(self._cols.datetime <= end_time)
                    filtered = 1
                case 'date_s':
                    start_time = datetime.fromisoformat(value)
                    db_query = db_query.where(self._cols.datetime >= start_time)
                    filtered = 1
                case 'date_e':
                    # 轉成 datetime，再加一天
                    end_time = datetime.strptime(value, "%Y-%m-%d") + timedelta(days=1)
                    db_query = db_query.where(self._cols.datetime < end_time)
                    filtered = 1
        return [db_query, filtered]

    def check_new_data(self, data):
        error_msgs = []
        # 新增檢查
        if 'datetime' not in data: error_msgs.append('請設定開始日期時間')
        if 'datetime2' not in data: error_msgs.append('請設定結束日期時間')

        # 一般檢查
        [error_msg2, data] = self.check_data(data)
        if error_msg2: error_msgs.append(error_msg2)
        return ['、'.join(error_msgs), data]

    def check_data(self, data):
        error_msgs = []
        if 'id' in data: del data['id']
        if 'created_at' in data: del data['created_at']
        if 'updated_at' in data: del data['updated_at']

        if 'datetime' in data and not data['datetime'].strip():
            error_msgs.append('請設定開始日期時間')
        if 'datetime2' in data and not data['datetime2'].strip():
            error_msgs.append('請設定結束日期時間')

        return ['、'.join(error_msgs), data]