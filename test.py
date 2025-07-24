import json
import os
from dotenv import load_dotenv
from lambda_function import lambda_handler

# 模擬 event/context
with open('test_event.json', 'r', encoding='utf-8') as f:
    event = json.load(f)
event['body'] = json.dumps(event['body'], ensure_ascii=False)

context = {}  # 可留空或放一些 mock context

# 設定環境變數(僅在本地開發時載入 .env)
if os.environ.get("AWS_EXECUTION_ENV") is None:
    load_dotenv()

result = lambda_handler(event, context)
print(result)
