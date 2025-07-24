from query.DBBase import DBBase
from sqlalchemy import Table, MetaData, select, or_, delete, insert
import re

class Users(DBBase):
    def __init__(self, engine, conn):
        self._engine = engine
        self._conn = conn

        metadata = MetaData()
        self._table = Table('users', metadata, autoload_with=self._engine)
        self._cols = self._table.c

    # 取得人員
    def get_data(self, where={}):
        # 設定撈取欄位&表
        db_query = select(
                        self._cols.id, 
                        self._cols.name, 
                        self._cols.name_line, 
                        self._cols.name_nick, 
                        self._cols.email, 
                        self._cols.cellphone, 
                        self._cols.gender, 
                        self._cols.level
                    ).select_from(self._table)

        # 設定篩選條件
        db_query = self.deal_where_query(db_query, where)

        # 執行sql
        result = self._conn.execute(db_query)
        
        # 轉成 list of dict
        rows = [dict(row._mapping) for row in result]

        # 添加sql語句到回傳資料(檢查用)
        # compiled = db_query.compile(compile_kwargs={"literal_binds": True})
        # rows.append(str(compiled))

        return {"data": rows}

    # 刪除人員
    def delete_data(self, where={}):
        # 設定刪除表
        db_query = delete(self._table)

        # 設定篩選條件
        db_query = self.deal_where_query(db_query, where)

        # 執行sql
        result = self._conn.execute(db_query)
        self._conn.commit()

        return result.rowcount

    # 編輯人員
    def update_data(self, where, data={}):
        pass
    
    # 新增人員
    def insert_data(self, data={}):
        msg = ''
        items = []
        if type(data) is list: # 批次新增
            for item in data:
                [error_msg, item] = self.check_data(item)
                if error_msg: msg += (item.get('name', '') + '：' + error_msg + "; ")
                items.append(item)
        else: # 單個新增
            [error_msg, data] = self.check_data(data)
            if error_msg: msg += (data.get('name', '') + '：' + error_msg + "; ")
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
                case 'email':
                    db_query = db_query.where(self._cols.email.like(f'%{value}%'))
                case 'cellphone':
                    db_query = db_query.where(self._cols.cellphone.like(f'%{value}%'))
                case 'gender':
                    db_query = db_query.where(self._cols.gender == value)
                case 'name_keyword':
                    db_query = db_query.where(
                        or_(
                            self._cols.name.like(f'%{value}%'),
                            self._cols.name_line.like(f'%{value}%'),
                            self._cols.name_nick.like(f'%{value}%')
                        )
                    )
                case 'level_over':
                    db_query = db_query.where(self._cols.level >= value)
        return db_query

    def check_data(self, data):
        error_msgs = []
        if 'id' in data: del data['id']
        if 'created_at' in data: del data['created_at']
        if 'updated_at' in data: del data['updated_at']

        if 'name' not in data: 
            error_msgs.append('請設定姓名')
        elif not data['name']:
            error_msgs.append('請設定姓名')
        if 'email' in data and data['email'] and not self.is_valid_email(data['email']):
            error_msgs.append('信箱格式有誤')
        if 'cellphone' in data and data['cellphone'] and not data['cellphone'].isdigit():
            error_msgs.append('手機號碼只可輸入數字')
        if 'gender' in data and data['gender'] not in [1,2]:
            error_msgs.append('性別設定有誤')
        if 'level' in data and data['level'] < 0:
            error_msgs.append('等級設定有誤')

        return ['、'.join(error_msgs), data]
    
    def is_valid_email(self, email):
        pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
        return re.match(pattern, email) is not None