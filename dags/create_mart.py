from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

import psycopg2

def get_postgres_connection():
    DBNAME = "orders-db"
    USER = "test"
    PASSWORD = "test"
    PORT = "5432"
    HOST = "orders-postgres-db"

    conn = psycopg2.connect(dbname=DBNAME, host=HOST, user=USER, password=PASSWORD, port=PORT)
    return conn

dag = DAG("create_mart", dagrun_timeout=timedelta(minutes=5), start_date=datetime(2025,3,16), schedule=timedelta(days=1), tags=["orders_system"])

def init_mart_tables(**kwargs):
    conn = get_postgres_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")

    check_result = cursor.fetchall()

    client_mart_table_exist = False
    monthly_order_mart_table_exist = False

    for table in check_result:
        if table[0] == 'monthly_order_mart':
            monthly_order_mart_table_exist = True
            print("Table daily_order_mart already exist")
        if table[0] == 'client_mart':
            client_mart_table_exist = True
            print("Table client_mart already exist")

    if not client_mart_table_exist:
        cursor.execute("CREATE TABLE client_mart (id text, name text, order_sum decimal, order_count decimal);")
        conn.commit()

    if not monthly_order_mart_table_exist:
        cursor.execute(
            "CREATE TABLE monthly_order_mart(year int, month int, order_sum decimal, order_count decimal);")
        conn.commit()

    cursor.close()
    conn.close()

def clear_mart_tables(**kwargs):
    conn = get_postgres_connection()
    cursor = conn.cursor()

    cursor.execute(
        "DELETE FROM client_mart")
    conn.commit()

    cursor.execute(
        "DELETE FROM monthly_order_mart")
    conn.commit()

    cursor.close()
    conn.close()

def create_client_mart(**kwargs):
    conn = get_postgres_connection()
    cursor = conn.cursor()

    mart_sql = """
    INSERT INTO client_mart(id, name, order_sum, order_count)
    SELECT
        id,
        name,
        (SELECT sum(amount) from orders where client_id = clients.id) as order_sum,
        (SELECT count(*) from orders where client_id = clients.id) as order_count
    FROM clients
    """

    cursor.execute(mart_sql)
    conn.commit()

    cursor.close()
    conn.close()

def create_monthly_order_mart(**kwargs):
    conn = get_postgres_connection()
    cursor = conn.cursor()

    mart_sql = """
         INSERT INTO monthly_order_mart(year, month, order_sum, order_count)
         SELECT date_part('year', created_at)::int as year, date_part('month', created_at)::int as month, sum(amount) as order_sum, count(*) as order_count
         FROM orders
         GROUP BY date_part('month', created_at), date_part('year', created_at)
         """

    cursor.execute(mart_sql)
    conn.commit()

    cursor.close()
    conn.close()


init_task = PythonOperator(
  task_id = 'init_mart_tables',
  python_callable=init_mart_tables,
  provide_context=True,
  dag=dag)

clear_mart_task = PythonOperator(
  task_id = 'clear_mart_tables',
  python_callable=clear_mart_tables,
  provide_context=True,
  dag=dag)

create_client_mart_task = PythonOperator(
  task_id = 'create_client_mart',
  python_callable=create_client_mart,
  provide_context=True,
  dag=dag)

create_monthly_order_mart_task = PythonOperator(
  task_id = 'create_monthly_order_mart',
  python_callable=create_monthly_order_mart,
  provide_context=True,
  dag=dag)

init_task >> clear_mart_task >> [create_client_mart_task, create_monthly_order_mart_task]