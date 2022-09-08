from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 append_data="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.append_data = append_data

    def execute(self, context):
        self.log.info("Connecting to redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_data == True:
            self.log.info(f"Loading data to dimension {self.table} table")
            dim_sql = self.sql[0]
            redshift.run(dim_sql)
            
        else:
            self.log.info(f"Clearing data from dimension {self.table} table")
            trunc_sql = self.sql[1]
            redshift.run(trunc_sql)
            
            self.log.info(f"Loading data to dimension {self.table} table")
            dim_sql = self.sql[0]
            redshift.run(dim_sql)
        
        self.log.info("Load finished")
