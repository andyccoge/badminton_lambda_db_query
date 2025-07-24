import json
import boto3
import os
import requests
from sqlalchemy import create_engine
from query import Users

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
    body = json.loads(event['body'])
    target = body.get('target', '')         # 資料對象
    where = body.get('where', {})           # 篩選條件
    data = body.get('data', {})             # 傳輸資料

    # 撈取資料
    try:
        match target:
            case 'users':
                Users_ins = Users.Users(engine, conn)
                if method=='POST': # 搜尋球員
                    rows = Users_ins.get_data(where)
                elif method=='DELETE': # 刪除球員
                    result = Users_ins.delete_data(where)
                    rows = {'deleted':result}
                elif method=='PUT': # 新增or編輯球員
                    if where: # 有篩選條件，視為編輯會員
                        rows = Users_ins.update_data(where, data)
                    else: # 無篩選條件，視為新增會員
                        rows = Users_ins.insert_data(data)
                else:
                    return {"statusCode": 403, "body": f"No this action:{method}"}
            
            case 'dates':
                pass
            
            case _:
                return {"statusCode": 500, "body": f"Wrong data target: {target}"}

        # 回傳 JSON 字串（用於 API 回傳）
        return {"statusCode": 200, "body": json.dumps(rows, ensure_ascii=False)}
    except Exception as e:
        return {"statusCode": 500, "body": f"DB operation error: {str(e)}"}
