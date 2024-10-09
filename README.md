# Modern Data Pipeline

A data pipeline example, using airbyte to move data from MongDB Atlas to a GCP bucket and Astro to move data from the bucket to a BigQuery. All orchestrated with Airflow.

## Technologies used
Airflow, Airbyte, Astro Python SDK, MongoDB, Google Cloud Storage, Google BigQuery


## DAG files
The dag files contain the code used to the orchestration

## Pre requisites

- Airflow server
- Airbyte account
- Google Cloud Platform account
- Python

## Configuring Airbyte
To move data you need to create and configure a connection to MongoDB and to GCP

### Airbyte
To create a GCP connection, first create a GCP service account, with permissions to read and write.
After that, create a GCP connection, in airbyte, and configure with your data.
Create the JSON file containing the account credentials
![GCP Connection configuration](/assets/airbyte-gcp-connection.png)

To create a MongoDB connection, first create a MongoDB Atlas account.
After that, create a MongoDB connection in airbyte and configure your data.

![MongoDB Connection configuration](/assets/airbyte-mongo-connection.png)
