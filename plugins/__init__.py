# Import necessary future functions for compatibility with Python 2 and 3
from __future__ import division, absolute_import, print_function

# Import Airflow's plugin manager
from airflow.plugins_manager import AirflowPlugin

# Import the custom operators and helper functions
import operators
import helpers

# Define an Airflow plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"  # The plugin name used internally in Airflow

    # Registering custom operators
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]

    # Registering helper functions (e.g., SQL queries)
    helpers = [
        helpers.SqlQueries
    ]
