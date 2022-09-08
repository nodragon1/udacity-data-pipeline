from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 param="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.param = param

    def execute(self, context):
        self.log.info("Connecting to redshift")     
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        self.log.info("Has row check")
        for row_check in self.param:
            table = row_check.get('table')
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
 
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
            
        error_count = 0
        self.log.info("Checking null")
        for null_check in self.param:
            table = null_check.get('table')
            sql = null_check.get('check_sql')
            exp_result = null_check.get('exp_result')
            result = redshift_hook.get_records(sql)
            
            if exp_result != result[0][0]:
                error_count += 1
                self.log.info(f"{result[0][0]} NULL in {table} table")
                
        if error_count >= 1:
            raise ValueError("Data quality check failed. NULL value")
            
        else:
            self.log.info("Data quality test passed")