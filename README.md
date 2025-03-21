# async_replication
Airflow example for async data replication from MongoDb to Postgres + creating marts.

## How to run:
Install Docker (I used Docker with Docker desktop app)
1) `docker compose build`
2) `docker compose up airflow-init`
3) `docker compose up`

Airflow will be accessible by `http://localhost:8080`

In DAGs list you can find several DAGs I created (tag = 'orders_system').

## How to generate test data:

To generate MongoDB test data run DAG `generate_data`

## How to replicate data:

Run DAG named `async_replicator`.

DAG `async_replicator` runs every 60 min 

## How to create marts:

Run DAG named `create_mart`.

DAG `create_mart` runs daily 

## Also:

### Postgres credentials:

* *DBNAME* = `orders-db`

* *USER* = `test`

* *PASSWORD* = `test`

* *PORT* = `5400`

* *HOST* = `localhost`

### MongoDB credentials:

* *SERVER* = `localhost`

* *PORT* = `27017`

* *DATABASE* = `orders-db`

* *USERNAME* = `mongouser`

* *PASSWORD* = `mongopasswd`
