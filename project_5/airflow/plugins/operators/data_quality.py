from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.sql_checks = kwargs['params']['sql_checks']

    def execute(self, context):
        self.log.info('Data checks...')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for check in self.sql_checks:
            sql = check.get('check_sql')
            compr = check.get('comparison')
            exp_result = check.get('expected_result')

            records = redshift.get_records(sql)[0]

            failing_tests = []
            error_count = 0

            if compr == 'eq':                
                if exp_result != records[0]:
                    error_count += 1
                    failing_tests.append(sql)
                    
            if compr == 'gt':
                if records[0] < exp_result:
                    error_count += 1
                    failing_tests.append(sql)


        if error_count > 0:
            self.log.info('The following data quality checks have failed: ')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')

        