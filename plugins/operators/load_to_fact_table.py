from airflow.hooks.postgres_hook import PostgresHook  # Allows us to connect to Redshift
from airflow.models import BaseOperator  # Inherits from Airflow's BaseOperator
from airflow.utils.decorators import apply_defaults  # Helps with initializing parameters


class LoadFactOperator(BaseOperator):
    """
    Airflow Operator to load data into a Fact Table in Redshift.
    """
    ui_color = '#F98866'  # Color for visualization in Airflow UI
    # SQL query to insert data into the 'songplays' fact table
    songplay_table_insert = """
        INSERT INTO songplays (playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)
        SELECT DISTINCT
            md5(events.ts::TEXT) AS songplay_id,  -- Generates a unique ID for each songplay
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        FROM 
            (SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time, *
             FROM staging_events
             WHERE page='NextSong') events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
           AND events.artist = songs.artist_name
           AND events.length = songs.duration
        WHERE (songs.song_id IS NOT NULL OR songs.artist_id IS NOT NULL)
        AND events.userid IS NOT NULL;
    """

    @apply_defaults
    def __init__(self, redshift_conn_id="", *args, **kwargs):
        """
        Initialize the LoadFactOperator.

        :param redshift_conn_id: The connection ID for Redshift in Airflow.
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """
        Execute the operator: Connect to Redshift and run the SQL query.
        """
        self.log.info('Connecting to Redshift...')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Loading data into the fact table (songplays)...')
        redshift.run(LoadFactOperator.songplay_table_insert)

        self.log.info('Fact table load complete!')
