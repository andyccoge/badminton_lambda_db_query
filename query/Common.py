from datetime import datetime

def datetimeToStamp(dt_str):
  parsed = None
  for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
    try:
        parsed = datetime.strptime(dt_str, fmt)
        break
    except ValueError:
        continue

  if parsed is None:
      raise ValueError(f"無效時間格式")

  return parsed