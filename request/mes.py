import requests
from datetime import datetime

headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json',
}
auth = ('admin', 'admin')
import json
body = {
  "conf": data,
  "dag_run_id": "string",
  "execution_date": '2020'+datetime.today().strftime('-%m-%dT%H:%M:%S.602Z')
}

req = requests.post("http://localhost:8080/api/v1/dags/update_data/dagRuns",
    headers=headers, auth=auth, data=json.dumps(body))
print(req.json())