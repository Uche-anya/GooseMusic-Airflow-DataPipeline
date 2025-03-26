import logging  # Used for logging messages in Airflow
from airflow.hooks.postgres_hook import PostgresHook  # Connects to Redshift
from airflow.models import BaseOperator  # Base class for custom Airflow operators
from airflow.utils.decorators import apply_defaults  # Allows default values for parameters


class LoadDimensionOperator(BaseOperator):
    """
    Custom Airflow Operator to load data into a dimension table in Amazon Redshift.

    Features:
    - Can either append new data or delete (truncate) old data before inserting new records.
    - Uses PostgresHook to connect to Redshift and execute SQL commands.
    """

    # Set UI color in Airflow
    ui_color = '#80BD9E'  # Greenish color in Airflow UI

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",  # Connection ID for Redshift (set in Airflow)
                 table_name="",  # Name of the dimension table
                 sql_statement="",  # SQL query to insert data
                 append_data=True,  # If True, keep old data; If False, delete old data first
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        # Store parameters for later use
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_statement = sql_statement
        self.append_data = append_data

    def execute(self, context):
        """
        Executes the SQL command to load data into the dimension table.
        """
        self.log.info(f'Loading data into {self.table_name} dimension table...')

        # Connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Check whether we need to keep (append) old data or delete (truncate) first
        if self.append_data:
            # Just insert new data without deleting old records
            sql_query = f'INSERT INTO {self.table_name} {self.sql_statement}'
            self.log.info(f'Appending data to {self.table_name}...')
        else:
            # First, delete (truncate) old data, then insert new data
            sql_query = f'TRUNCATE TABLE {self.table_name}; INSERT INTO {self.table_name} {self.sql_statement}'
            self.log.info(f'Truncating {self.table_name} before inserting new data...')

        # Run the SQL command in Redshift
        redshift.run(sql_query)
        self.log.info(f'Successfully loaded data into {self.table_name}')
