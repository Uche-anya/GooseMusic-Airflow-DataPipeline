import logging  # Used for logging messages in Airflow
from airflow.hooks.postgres_hook import PostgresHook  # Connects to Redshift
from airflow.models import BaseOperator  # Base class for custom Airflow operators
from airflow.utils.decorators import apply_defaults  # Allows default values for parameters


class DataQualityOperator(BaseOperator):
    """
    Custom Airflow Operator to perform data quality checks in Amazon Redshift.

    Features:
    - Runs a SQL query to check data quality.
    - Compares the result against an expected value.
    - If the check fails, it raises an error to stop the pipeline.

    """

    # Set UI color in Airflow
    ui_color = '#89DA59'  # Light green color in Airflow UI

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",  # Connection ID for Redshift (set in Airflow)
                 check_sql="",  # SQL query to check data quality
                 expected_value=0,  # Expected result of the SQL query
                 describe="",  # Description of the check (for logging)
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        # Store parameters for later use
        self.redshift_conn_id = redshift_conn_id
        self.check_sql = check_sql
        self.expected_value = expected_value
        self.describe = describe

    def execute(self, context):
        """
        Executes the SQL query and checks if the data quality condition is met.
        """
        self.log.info(f'Running Data Quality Check: {self.describe}')

        # Connect to Redshift
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # Log the expected value and SQL query
        self.log.info("\n".join([
            f"Checking: {self.describe}",
            f"Expected value: {self.expected_value}",
            f"SQL Query: {self.check_sql}"
        ]))

        # Run the SQL query in Redshift
        records = redshift_hook.get_records(self.check_sql)

        # Check if the query returned any records
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data Quality Check FAILED: No records returned for {self.describe}")

        actual_value = records[0][0]  # Extract the first value from the result

        # Compare actual vs expected value
        if int(actual_value) != int(self.expected_value):
            raise ValueError(f"Data Quality Check FAILED for {self.describe}.\n"
                             f"Expected: {self.expected_value}\n"
                             f"Actual: {actual_value}")

        # If the check passes, log success
        self.log.info(f"Data Quality Check PASSED for {self.describe}.\n"
                      f"Expected: {self.expected_value}\n"
                      f"Actual: {actual_value}")
