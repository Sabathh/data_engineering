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

        self.table = kwargs['params']['table']
        self.fields = kwargs['params']['fields']

    def execute(self, context):
        self.log.info(f'Loading data into {self.table} (facts table)')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        redshift.run(f"INSERT INTO {self.table} ({','.join(self.fields)}) {self.sql}")
        