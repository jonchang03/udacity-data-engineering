from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Load dimension tables using a truncate-insert pattern 
    where the target table is emptied before the load.
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 truncate=True,
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.truncate = truncate
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.truncate:
            self.log.info(f"Emptying the following table: {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")
            
        self.log.info(f"Loading the following dimension table: {self.table}")
        redshift.run(self.sql_query)
        self.log.info(f"Following dimension table successfully loaded: {self.table}")