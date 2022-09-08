from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

from helpers import SqlQueries

#
# The following argument is for...
#
#       1. Not having dependencies on past runs
#       2. On failure, the task are retried 3 times
#       3. Retries happen every 5 minutes
#       4. Catchup is turned off
#       5. Not emailing on retry
#
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

#
# The following DAG performs the following functions:
#
#       1. Loads song and log data from S3 to RedShift
#       2. Insert data from copy table to fact-dimension tables
#       3. Performs a data quality check on the fact-dimension tables in RedShift
#       
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Loads song and log data from S3 to RedShift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    format_as_json='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    format_as_json='auto'
)

# Insert data from copy table to fact-dimension tables
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    dag=dag,
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table='users',
    dag=dag,
    sql=[SqlQueries.user_table_insert, SqlQueries.truncate_user_table],
    append_data=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    table='songs',
    dag=dag,
    sql=[SqlQueries.song_table_insert, SqlQueries.truncate_song_table],
    append_data=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    table='artists',
    dag=dag,
    sql=[SqlQueries.artist_table_insert, SqlQueries.truncate_artist_table],
    append_data=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table='time',
    dag=dag,
    sql=[SqlQueries.time_table_insert, SqlQueries.truncate_time_table],
    append_data=True
)

# Data quality check on fact and dimension tables
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    param=[{'table':'songplays',
            'check_sql':'SELECT COUNT(*) FROM songplays WHERE playid IS NULL;',
            'exp_result':0},
           {'table':'users',
            'check_sql':'SELECT COUNT(*) FROM users WHERE userid IS NULL;',
            'exp_result':0},
           {'table':'songs',
            'check_sql':'SELECT COUNT(*) FROM songs WHERE songid IS NULL;',
            'exp_result':0},
           {'table':'artists',
            'check_sql':'SELECT COUNT(*) FROM artists WHERE artistid IS NULL;',
            'exp_result':0},
           {'table':'time',
            'check_sql':'SELECT COUNT(*) FROM time WHERE start_time IS NULL;',
            'exp_result':0}],      
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task ordering for the DAG tasks 
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator