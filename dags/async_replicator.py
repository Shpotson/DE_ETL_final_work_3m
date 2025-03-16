from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

import psycopg2
import pymongo

def get_postgres_connection():
    DBNAME = "orders-db"
    USER = "test"
    PASSWORD = "test"
    PORT = "5432"
    HOST = "orders-postgres-db"

    conn = psycopg2.connect(dbname=DBNAME, host=HOST, user=USER, password=PASSWORD, port=PORT)
    return conn

def get_mongo_connection():
    DBNAME = 'orders-db'
    USER = 'mongouser'
    PASSWORD = 'mongopasswd'
    PORT = 27017
    HOST = 'orders-mongodb'
    client = pymongo.MongoClient(HOST, PORT, username=USER, password=PASSWORD)

    db_connection = client[DBNAME]
    return db_connection

dag = DAG("async_replicator", dagrun_timeout=timedelta(minutes=5), start_date=datetime(2025,3,14, hour=22), schedule=timedelta(minutes=60), tags=["orders_system"])

def init_tables(**kwargs):
    conn = get_postgres_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")

    check_result = cursor.fetchall()

    clients_table_exist = False
    orders_table_exist = False

    for table in check_result:
        if table[0] == 'orders':
            orders_table_exist = True
            print("Table orders already exist")
        if table[0] == 'clients':
            clients_table_exist = True
            print("Table clients already exist")

    if not clients_table_exist:
        cursor.execute("CREATE TABLE clients(id text PRIMARY KEY, name text);")
        conn.commit()

    if not orders_table_exist:
        cursor.execute(
            "CREATE TABLE orders(id text PRIMARY KEY, amount decimal, client_id text REFERENCES clients(id), created_at timestamp);")
        conn.commit()

    cursor.close()
    conn.close()

def data_replication_orders(**kwargs):
    pg_conn = get_postgres_connection()
    mongo_db = get_mongo_connection()

    pg_cursor = pg_conn.cursor()

    orders_collection = mongo_db['orders']

    offset = 0
    while True:
        cursor = orders_collection.find().skip(offset).limit(10)

        offset += 10

        count = 0

        for order in cursor:
            count += 1

            id = order["id"]
            amount = order["amount"]
            created_at = order["created_at"]
            client_id = order["client"]["id"]
            client_name = order["client"]["name"]

            clientInsertQuery = f"INSERT INTO clients(id, name) VALUES ('{client_id}', '{client_name}') ON CONFLICT (id) DO NOTHING;"
            pg_cursor.execute(clientInsertQuery)

            pg_conn.commit()

            orderInsertQuery = f"INSERT INTO orders(id, amount, client_id, created_at) VALUES ('{id}', {amount}, '{client_id}', '{created_at}') ON CONFLICT (id) DO NOTHING;"
            pg_cursor.execute(orderInsertQuery)

            pg_conn.commit()

        if count < 10:
            break

        print(f"Replicated {count} orders")

        offset += 10

    pg_cursor.close()


init_task = PythonOperator(
  task_id = 'init_tables',
  python_callable=init_tables,
  dag=dag)

data_replication_orders_task = PythonOperator(
  task_id = 'data_replication_orders',
  python_callable=data_replication_orders,
  dag=dag)

init_task >> data_replication_orders_task