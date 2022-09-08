from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        
    def execute(self, context):
        self.log.info("Connecting to redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        fact_sql = self.sql
        
        self.log.info("Loading data to fact table")
        redshift.run(fact_sql)
        
        self.log.info("Load finished")
        
