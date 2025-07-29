import json
import boto3
import os
# import requests
from sqlalchemy import create_engine
from query import Users, PlayDate, Courts, Reservations, Matchs
from query.derivation import PlayDateData

db_name = 'badminton_db_1'

def get_secret(secret_name, region_name="ap-northeast-1"):
    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def get_db_connect():
    db_host = os.environ.get('DB_HOST')
    db_port = int(os.environ.get('DB_PORT'))

    # 從 AWS Secrets Manager 取得 DB 資訊
    secret_name = os.environ.get("DB_SECRET_NAME", "")
    secret = get_secret(secret_name)
    db_user = secret["username"]
    db_password = secret["password"]

    engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    conn = engine.connect()
    return [engine, conn]


def lambda_handler(event, context):
    # 建立連線
    try:
        [engine, conn] = get_db_connect()
    except Exception as e:
        return {"statusCode": 500, "body": f"Database connection error: {str(e)}"}

    # 解析請求內容
    method = event.get('httpMethod', '')    # 請求方式
    method = method.upper()
    body = event.get('body', {})
    target = body.get('target', '')         # 資料對象
    where = body.get('where', {})           # 篩選條件
    data = body.get('data', {})             # 傳輸資料

    # 撈取資料
    try:
        match target:
            case 'users':
                Users_ins = Users.Users(engine, conn)
                if method=='GET': # 搜尋球員
                    result = Users_ins.get_data(where)
                elif method=='DELETE': # 刪除球員
                    result = Users_ins.delete_data(where)
                elif method=='POST': # 新增球員
                    result = Users_ins.insert_data(data)
                elif method=='PUT': # 編輯球員
                    result = Users_ins.update_data(where, data)
                else:
                    return {"statusCode": 403, "body": f"No this action:{method}"}
            
            case 'play_date':
                PlayDate_ins = PlayDate.PlayDate(engine, conn)
                if method=='GET': # 搜尋打球日
                    result = PlayDate_ins.get_data(where)
                elif method=='DELETE': # 刪除打球日
                    result = PlayDate_ins.delete_data(where)
                elif method=='POST': # 新增打球日
                    result = PlayDate_ins.insert_data(data)
                elif method=='PUT': # 編輯打球日
                    result = PlayDate_ins.update_data(where, data)
                else:
                    return {"statusCode": 403, "body": f"No this action:{method}"}
            
            case 'courts':
                Courts_ins = Courts.Courts(engine, conn)
                if method=='GET': # 搜尋場地
                    result = Courts_ins.get_data(where)
                elif method=='DELETE': # 刪除場地
                    result = Courts_ins.delete_data(where)
                elif method=='POST': # 新增場地
                    result = Courts_ins.insert_data(data)
                elif method=='PUT': # 編輯場地
                    result = Courts_ins.update_data(where, data)
                else:
                    return {"statusCode": 403, "body": f"No this action:{method}"}

            case 'reservations':
                Reservations_ins = Reservations.Reservations(engine, conn)
                if method=='GET': # 搜尋報名紀錄
                    result = Reservations_ins.get_data(where)
                elif method=='DELETE': # 刪除報名紀錄
                    result = Reservations_ins.delete_data(where)
                elif method=='POST': # 新增報名紀錄
                    result = Reservations_ins.insert_data(data)
                elif method=='PUT': # 編輯報名紀錄
                    result = Reservations_ins.update_data(where, data)
                else:
                    return {"statusCode": 403, "body": f"No this action:{method}"}

            case 'matchs':
                Matchs_ins = Matchs.Matchs(engine, conn)
                if method=='GET': # 搜尋比賽紀錄
                    result = Matchs_ins.get_data(where)
                elif method=='DELETE': # 刪除比賽紀錄
                    result = Matchs_ins.delete_data(where)
                elif method=='POST': # 新增比賽紀錄
                    result = Matchs_ins.insert_data(data)
                elif method=='PUT': # 編輯比賽紀錄
                    result = Matchs_ins.update_data(where, data)
                else:
                    return {"statusCode": 403, "body": f"No this action:{method}"}

            case 'play_date_data':
                PlayDateData_ins = PlayDateData.PlayDateData(engine, conn)
                if method=='GET': # 取得打球日所需的所有資料(含場地、報名紀錄、比賽紀錄、相關人員)
                    result = PlayDateData_ins.get_data(where)
                else:
                    return {"statusCode": 403, "body": f"No this action:{method}"}

            case _:
                return {"statusCode": 403, "body": f"Wrong data target: {target}"}

        # 回傳 JSON 字串（用於 API 回傳）
        return {"statusCode": 200, "body": result}
    except Exception as e:
        raise
        return {"statusCode": 500, "body": f"DB operation error: {str(e)}"}
