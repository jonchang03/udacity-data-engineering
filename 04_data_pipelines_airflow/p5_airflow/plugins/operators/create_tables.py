from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTablesOperator(BaseOperator):
    """
    Creates tables in Redshift using the create_tables.sql script.
    """
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Creating Redshift tables...")
        
        sql_file = open('/home/workspace/airflow/create_tables.sql', 'r').read()
        sql_commands = sql_file.split(';')

        for command in sql_commands:
            if command.rstrip() != '':
                redshift.run(command)
                
        self.log.info("Tables have been successfully created.")