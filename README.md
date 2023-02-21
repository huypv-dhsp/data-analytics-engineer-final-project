# Set up local
## Setup Docker image
- run $ docker build . -t airflow-dbt:2.5.0-3.10
- run $ docker-compose up
## Setup Postgres Connection
- From Airflow UI
- Click on Admin - Connections, then click on + icon
- Fill with information:
    + Connection id: thelook_postgres_connection
    + Connection type: Postgres
    + Host: 34.136.101.135
    + Schema: thelook_ecommerce
    + Login: postgres
    + Password: <postgres password>
    + Port: 5432
- Then click Save button

## Setup Google Cloud Connection
- From Airflow UI
- Click on Admin - Connections, then click on + icon
- Fill with information:
    + Connection id: google_cloud_default
    + Connection type: Google Cloud
    + Project id: <Google Cloud project id>
- Then click Save button
