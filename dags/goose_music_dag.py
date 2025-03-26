from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
import sql_statements

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


# Default arguments for the DAG
# These settings apply to all tasks unless overridden
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 24),  # DAG starts from this date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # Number of retries in case of failure
    'retry_delay': timedelta(minutes=1),  # Delay before retrying
    'catchup': False,  # Do not run past scheduled runs when started
}

# Define the DAG
# This pipeline loads and transforms data in Redshift using Airflow
# Runs daily at midnight (UTC)
dag = DAG(
    'Sparkify_Data_Pipeline_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 0 * * *'
)

# Start task - Dummy operator to mark the beginning
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# Create tables in Redshift for staging events data
create_staging_events_table = PostgresOperator(
    task_id="create_staging_events_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_staging_events_TABLE_SQL
)

# Load event data from S3 to Redshift staging table
stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events_from_s3_to_redshift",
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    JSONPaths="log_json_path.json"
)

# Create tables in Redshift for staging songs data
create_staging_songs_table = PostgresOperator(
    task_id="create_staging_songs_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_staging_songs_TABLE_SQL
)

# Load song data from S3 to Redshift staging table
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs_from_s3_to_redshift",
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    JSONPaths="auto"
)

# Load fact table from staging tables
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift"
)

# Load dimension tables from staging tables
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="users",
    sql_statement=sql_statements.user_table_insert,
    append_data=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="songs",
    sql_statement=sql_statements.song_table_insert,
    append_data=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="artists",
    sql_statement=sql_statements.artist_table_insert,
    append_data=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="time",
    sql_statement=sql_statements.time_table_insert,
    append_data=True
)

# Run data quality checks
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    check_sql="SELECT COUNT(*) FROM songplays",
    expected_value="320",
    describe="Fact table songplay - whether this table has data or not!"
)

# End task - Dummy operator to mark the end
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Define task dependencies
start_operator >> create_staging_events_table
start_operator >> create_staging_songs_table

create_staging_events_table >> stage_events_to_redshift
create_staging_songs_table >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table
]

[
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table
] >> run_quality_checks

run_quality_checks >> end_operator
