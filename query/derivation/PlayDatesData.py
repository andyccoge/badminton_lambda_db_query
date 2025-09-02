from query.PlayDate import PlayDate
from sqlalchemy import Table, MetaData, select, distinct, desc, func

class PlayDatesData(PlayDate):
  def __init__(self, engine, conn):
    super().__init__(engine, conn)

    metadata = MetaData()
    self._courts = Table('courts', metadata, autoload_with=self._engine)
    self._reservations = Table('reservations', metadata, autoload_with=self._engine)

  # 取得報名紀錄
  def get_data(self, where={}):
    # 子查詢：計算 courts type=1 的數量
    courts_subq = (
        select(
            self._courts.c.play_date_id,
            func.count(self._courts.c.id).label("count_courts_type1")
        )
        .where(self._courts.c.type == 1)
        .group_by(self._courts.c.play_date_id)
    ).subquery()

    # 子查詢：計算 reservations 數量
    reservations_subq = (
        select(
            self._reservations.c.play_date_id,
            func.count(self._reservations.c.id).label("count_reservations")
        )
        .group_by(self._reservations.c.play_date_id)
    ).subquery()

    # 主查詢：以 _table 為基準
    db_query = (
      select(
          self._cols.id,
          self._cols.location,
          self._cols.note,
          func.date_format(self._cols.datetime, "%Y-%m-%d %H:%i").label("datetime"),
          func.date_format(self._cols.datetime2, "%Y-%m-%d %H:%i").label("datetime2"),
          func.coalesce(courts_subq.c.count_courts_type1, 0).label("count_courts"),
          func.coalesce(reservations_subq.c.count_reservations, 0).label("count_reservations"),
      )
      .select_from(
          self._table
          .outerjoin(courts_subq, courts_subq.c.play_date_id == self._cols.id)
          .outerjoin(reservations_subq, reservations_subq.c.play_date_id == self._cols.id)
      )
    )

    # 設定篩選條件
    [db_query, *_] = self.deal_where_query(db_query, where)
    # 設定排序
    db_query = db_query.order_by(desc(self._cols.datetime))

    # 執行sql
    result = self._conn.execute(db_query)
    
    # 轉成 list of dict
    rows = [dict(row._mapping) for row in result]

    return {"data": rows}