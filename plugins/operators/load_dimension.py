from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 create_table_sql="",
                 table="",
                 values="",
                 insert_mode="truncate",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_table_sql = create_table_sql
        self.table = table
        self.values = values
        self.insert_mode = insert_mode

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.create_table_sql:
            self.log.info(f'Creating {self.table} table if it does not exist')
            redshift.run(self.create_table_sql)

        if self.insert_mode == 'truncate':
            self.log.info(f'Deleting all rows from {self.table}')
            redshift.run(f'TRUNCATE {self.table}')

        self.log.info('Inserting data into dimension table')
        reshift.run(f'INSERT INTO {self.table} {self.values}')
