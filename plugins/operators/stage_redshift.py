from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)

    @apply_defaults
    def __init__(self,              
                 redshift_conn_id="",
                 aws_credentials_id="",
                 create_table_sql="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 data_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.create_table_sql = create_table_sql
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.data_format = data_format

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if create_table_sql:
            self.log.info('Creating destination table if it does not exist')
            redshift.run(f'{self.create_table_sql}')

        self.log.info('Clearing data from destination table')
        redshift.run(f'DELETE FROM {self.table}')

        self.log.info('Copying data from S3 to Redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{rendered_key}'
        formatted_sql = f"""COPY {self.table}
                            FROM {s3_path}
                            FORMAT AS {self.data_format} 'auto'
                            ACCESS_KEY_ID '{credentials.access_key}'
                            SECRET_ACCESS_KEY '{credentials.secret_access_key}';
                            """





