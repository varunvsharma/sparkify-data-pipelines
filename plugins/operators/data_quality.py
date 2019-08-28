from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
        self.table = table

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Look for records in table list')
        if isinstance(self.table, list):
            for t in self.table:
                records = redshift.get_records(f'SELECT COUNT(*) FROM {t}')

                # Check if any records exist in the target table
                if records is None or len(records) < 1:
                    raise ValueError(f'No records in table {t}')
                else:
                    self.log(f'Data quality check on table {t} passed with {records[0][0]} records')
        else:
            records = redshift.get_records(f'SELECT COUNT(*) FROM {self.table}')
            
            # Check if any records exist in the target table
            if records is None or len(records) < 1:
                raise ValueError(f'No records in table {self.table}') 
            else:
                self.log(f'Data quality check on table {self.table} passed with {records[0][0]} records')

                