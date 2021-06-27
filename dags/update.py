import airflow.utils.dates
from airflow import DAG, utils
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pyodbc
from datetime import datetime, timedelta

'''
1) Получили новую операцию с типом typeID для аппаратуры Num
2) Ищем в OperationDeadlines запись, где совпадает typeID b Num
3) В записи хвренится prevOpID
4) Из предыдущей записи (prevOpID) получаем дату prevDate
5) Из SlaRules получаем запись, где typeID- тип предыдущей операции. NextTypeID - тип текущей операции. Из этой записи достаем duration
6) Нужное значение - дата текущей операции - дата предыдущей операции + duration
7) Удаляем запись из OperationDeadlines
'''

dag = DAG(
    dag_id='update_data',
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None
)

def _push_to_db(**context):
    return context["dag_run"].conf

def _update_state(data):
    server = 'connect.ineutov.me, 1433' 
    database = 'GPN' 
    username = 'sa' 
    password = 'P@ssw0rd' 
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    cursor.execute(f"SELECT ID FROM OperationDeadlines WHERE typeId={data['typeId']} AND equipmentNumber=\'{data['equipmentNumber']}\'")
    prevId = cursor.fetchall()[0][0]
    cursor.execute(f"SELECT date FROM Operations WHERE id={prevId}")
    prevDate = datetime.strptime(cursor.fetchall()[0][0], '%Y-%m-%d %H:%M:%S')
    cursor.execute(f"SELECT duration FROM SlaRules WHERE typeId={prevId} AND NextTypeID={data['typeId']}")
    duration = timedelta(seconds=int(cursor.fetchall()[0][0]))
    req_value = int((data['date'] - prevDate + duration).total_seconds())
    cursor.execute(f"INSERT INTO Operations (equipmentNumber, performer, postponedTime) VALUES(\'{data['equipmentNumber']}\', \'{data['performer']}\', {req_value})")
    cnxn.commit()
    cursor.close()
    cnxn.close()

# 1)
get_context = PythonOperator(
    task_id='get_context',
    python_callable=_push_to_db,
    dag=dag
)

# all
find_element = PythonOperator(
    task_id='find_element',
    python_callable=_update_state,
    dag=dag
)
