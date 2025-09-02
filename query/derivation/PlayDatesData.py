import json
from query import PlayDate
from sqlalchemy import Table, MetaData, select, distinct, func

class PlayDatesData(PlayDate):
  def __init__(self, engine, conn):
    super.__init__(engine, conn)
    
    metadata = MetaData()
    self._courts = Table('courts', metadata, autoload_with=self._engine)
    self._reservations = Table('reservations', metadata, autoload_with=self._engine)

  # 取得報名紀錄
  def get_data(self, where={}):
    # 設定撈取欄位&表
    db_query = select(
                self._cols.id,
                self._cols.location,
                self._cols.note,
                func.date_format(self._cols.datetime, "%Y-%m-%d %H:%i").label("datetime"),
                func.date_format(self._cols.datetime2, "%Y-%m-%d %H:%i").label("datetime2"),
                func.count(distinct(self._courts.c.id)).label("count_courts"),
                func.count(distinct(self._reservations.c.id)).label("count_reservations"),
              ).select_from(
                self._table
                .join(self._courts, self._courts.c.play_date_id == self._cols.id, isouter=True)
                .join(self._reservations, self._reservations.c.play_date_id == self._cols.id, isouter=True)
              ).group_by(
                self._cols.id,
                self._cols.location,
                self._cols.note,
                self._cols.datetime,
                self._cols.datetime2,
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