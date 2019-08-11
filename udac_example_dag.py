from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from helpers import create_tables

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 6, 4),    
    'depends_on_past': False,
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2019, 6, 5, 16, 0),
    'catchup': False
}

   
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = "staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="s3://udacity-dend/log_data",
    file_format = "json",
    csv_format  = "csv",
    json_path = "s3://udacity-dend/log_json_path.json",
    sql = create_tables.staging_events    
 )

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = "staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="s3://udacity-dend/song_data",
    file_format = "json",
    csv_format  = "csv",
    json_path = "auto",
    sql = create_tables.staging_songs    
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table = "songplays",
    create_table_sql = create_tables.songplays,
    insert_table_sql = SqlQueries.songplay_table_insert,
    redshift_conn_id="redshift"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table = "users",
    create_table_sql = create_tables.users,
    insert_table_sql = SqlQueries.user_table_insert,
    redshift_conn_id="redshift",
    load_type = "delete-load" 
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,    
    table = "songs",
    create_table_sql = create_tables.songs,
    insert_table_sql = SqlQueries.song_table_insert,
    redshift_conn_id="redshift",
    load_type = "delete-load"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,    
    table = "artists",
    create_table_sql = create_tables.artists,
    insert_table_sql = SqlQueries.artist_table_insert,
    redshift_conn_id="redshift",
    load_type = "delete-load"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,   
    table = "time",
    create_table_sql = create_tables.time_table,
    insert_table_sql = SqlQueries.time_table_insert,
    redshift_conn_id="redshift",
    load_type = "delete-load"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    params={"users_data_check": "SELECT COUNT(userid) FROM users WHERE userid IS NULL;",
            "songs_data_check": "SELECT COUNT(songid) FROM songs WHERE songid IS NULL;",
            "artists_data_check": "SELECT COUNT(artistid) FROM artists WHERE artistid IS NULL;",
            "time_data_check": "SELECT COUNT(start_time) FROM time WHERE start_time IS NULL;",
            "users_data_result":0,
            "songs_data_result":0,
            "artists_data_result":0,
            "time_data_result":0
           } 
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift 
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator