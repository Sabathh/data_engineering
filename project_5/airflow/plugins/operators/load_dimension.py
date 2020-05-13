from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql = "",
                 truncate="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        
        self.sql = sql
        self.truncate = truncate

        self.table = kwargs['params']['table']
        self.fields = kwargs['params']['fields']

    def execute(self, context):
        self.log.info(f'Loading data into {self.table} (dimensions table)')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.table}")

        redshift.run(f"INSERT INTO {self.table} ({','.join(self.fields)}) {self.sql}")
        #redshift.run(self.sql)