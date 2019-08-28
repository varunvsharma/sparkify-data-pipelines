from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 create_table_sql="",
                 table="",
                 values="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_table_sql = create_table_sql
        self.table = table
        self.values = values

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.create_table_sql:
            self.log.info('Creating fact table if it does not exist')
            redshift.run(self.create_table_sql)

        self.log.info('Inserting data into fact table')
        redshift.run(f'INSERT INTO {self.table} {self.values}')
