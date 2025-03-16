from random import random

from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator

import uuid
from faker import Faker
import pymongo

dag = DAG("generate_data", dagrun_timeout=timedelta(minutes=5), tags=["orders_system"])

def get_mongo_connection():
    DBNAME = 'orders-db'
    USER = 'mongouser'
    PASSWORD = 'mongopasswd'
    PORT = 27017
    HOST = 'orders-mongodb'

    client = pymongo.MongoClient(HOST, PORT, username=USER, password=PASSWORD)

    db_connection = client[DBNAME]

    return db_connection

def datagen(**kwargs):
    fake_generator = Faker()

    db = get_mongo_connection()

    orders_collection = db['orders']

    client_ids = []

    for i in range(500):
        id = uuid.uuid4()
        client_ids.append(str(id))

    for i in range(5000):
        index = int(random() * 499)
        amount = random() * 2000 + 500
        id = uuid.uuid4()
        client_id = client_ids[index]
        client_name =  fake_generator.name()
        created_at = fake_generator.date_time()

        order = {
            "id": str(id),
            "amount": amount,
            "created_at": created_at.isoformat(),
            "client": {
                "id": client_id,
                "name": client_name,
            }
        }

        orders_collection.insert_one(order)



datagen_task = PythonOperator(
  task_id = 'datagen',
  python_callable=datagen,
  provide_context=True,
  dag=dag)