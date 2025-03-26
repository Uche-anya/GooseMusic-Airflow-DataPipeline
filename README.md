# Goose Music Data Pipeline

## Project Overview

Goose Music is a data pipeline project that processes and analyzes music streaming event data using AWS Redshift and Apache Airflow. The pipeline extracts raw JSON data from an S3 bucket, stages it in Redshift, and transforms it into a star schema optimized for analytical queries.

## Data Source

The raw data used in this project is provided by Udacity and consists of user activity logs and song metadata.

## Technologies Used

- **AWS Redshift**: Cloud data warehouse to store and process large datasets.
- **Apache Airflow (Docker)**: Workflow automation for scheduling and orchestrating ETL processes.
- **Python & SQL**: Data transformation and querying.
- **Amazon S3**: Cloud storage for raw and staged data.

## Project Setup Instructions

### 1. Set Up AWS Redshift Cluster

1. Log in to your AWS account.
2. Navigate to the **Redshift** service and create a new cluster.
3. Choose a free-tier-eligible or appropriate node type.
4. Set database name, username, and password.
5. Open **VPC security group settings** and allow inbound traffic on port `5439` (Redshift default port).
6. Note down the **host endpoint** of your Redshift cluster.

### 2. Configure AWS Credentials

Make sure your AWS credentials are properly set up for Airflow to access S3 and Redshift.

- Store the credentials in `~/.aws/credentials` or .env:
  ```ini
  [default]
  aws_access_key_id=YOUR_ACCESS_KEY
  aws_secret_access_key=YOUR_SECRET_KEY
  ```

### 3. Set Up Airflow on Docker

1. Install **Docker** and **Docker Compose** if not already installed.
2. Clone this repository:
   ```sh
   git clone https://github.com/Uche-anya/GooseMusic-Airflow-DataPipeline.git
   cd GooseMusic-Airflow-DataPipeline
   ```
3. Start Airflow using Docker:
   ```sh
   docker-compose up -d
   ```
4. Access the Airflow UI at `http://localhost:8080` and configure your connections:
   - `aws_credentials` (AWS key and secret)
   - `redshift` (Redshift cluster details)

### 4. Run the Pipeline

1. In the **Airflow UI**, enable and trigger the `goose_music_dag`.
2. Monitor task execution in the **DAGs** tab.

## Licensing

The raw data used in this project is sourced from Udacity and is subject to their licensing terms. This project itself is open-source and free for educational use.

