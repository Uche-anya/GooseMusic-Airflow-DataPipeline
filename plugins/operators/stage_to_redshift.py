from airflow.hooks.postgres_hook import PostgresHook  # Connects to Redshift
from airflow.models import BaseOperator  # Base class for all Airflow Operators
from airflow.utils.decorators import apply_defaults  # Handles default arguments
from airflow.contrib.hooks.aws_hook import AwsHook  # Connects to AWS for credentials


class StageToRedshiftOperator(BaseOperator):
    """
    Custom Airflow Operator to load JSON data from S3 into Amazon Redshift.
    This operator uses Redshift's COPY command to efficiently load data.
    """
    # Set UI color for the task in Airflow DAGs
    ui_color = '#358140'
    # Template fields allow dynamic values using Airflow's templating engine
    template_fields = ("s3_key",)
    # SQL COPY command to load data from S3 into Redshift
    copy_sql = """
        COPY {}  -- Target table in Redshift
        FROM '{}'  -- S3 file path
        ACCESS_KEY_ID '{}'  -- AWS credentials
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'  -- Define JSON structure
        TIMEFORMAT AS 'epochmillisecs'  -- Convert timestamp format
        region 'us-west-2'  -- AWS region where S3 bucket is stored
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",  # Redshift connection ID (Airflow)
                 aws_credentials_id="",  # AWS credentials ID (Airflow)
                 table="",  # Target table in Redshift
                 s3_bucket="",  # S3 bucket name
                 s3_key="",  # S3 key (file path)
                 JSONPaths="",  # JSON Path file for schema
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        # Store parameters for later use
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.JSONPaths = JSONPaths

    def execute(self, context):
        """ Executes the COPY command to load data into Redshift """

        # Get AWS credentials from Airflow
        self.log.info("Fetching AWS credentials...")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        # Connect to Redshift
        self.log.info("Connecting to Redshift...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear existing data in the target table (if needed)
        self.log.info(f"Clearing data from Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        # Format S3 path dynamically based on execution context
        rendered_key = self.s3_key.format(**context)  # Handles templated values
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"  # Full S3 path
        json_path = f"s3://{self.s3_bucket}/{self.JSONPaths}"  # JSON Paths file

        # Handle case where JSON Path is 'auto' for song data
        if rendered_key == "song_data":
            json_path = self.JSONPaths  # Set to 'auto' for auto schema detection

        # Format final SQL COPY command
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            json_path
        )

        # Log and execute the COPY command
        self.log.info(f"Running COPY command: {formatted_sql}")
        redshift.run(formatted_sql)
        self.log.info("Data successfully staged to Redshift!")
