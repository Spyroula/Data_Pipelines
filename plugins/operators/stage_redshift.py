from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_query = " COPY {} \
    FROM '{}' \
    ACCESS_KEY_ID '{}' \
    SECRET_ACCESS_KEY '{}' \
    FORMAT AS json '{}'; \
    "

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credential_id="",
                 table_name = "",
                 s3_bucket="",
                 s3_key = "",
                 file_format = "",
                 log_json_file = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.log_json_file = log_json_file
        self.execution_date = kwargs.get('execution_date') 

    def execute(self, context):
        """
        Extract data from S3 buckets into staging tables in Redshift cluster .
        
        Parameters
        -----------
        redshift_conn_id: str
            The Redshift cluster connection
        aws_credentials_id: str
            Tge AWS connection credentials
        table_name: str
             The name of the table in the Redshift cluster 
        s3_bucket: str
            The name of the S3 bucket holding the source data
        s3_key: str
            The S3 key files of the source data
        file_format: str
            The format of the source file e.g. JSON, CSV
        """
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info(f"Select the staging files for the {self.table_name} table from the location : {s3_path}")
        
        if self.log_json_file != "":
            self.log_json_file = "s3://{}/{}".format(self.s3_bucket, self.log_json_file)
            copy_query = self.copy_query.format(self.table_name, s3_path, credentials.access_key, credentials.secret_key, self.log_json_file)
        else:
            copy_query = self.copy_query.format(self.table_name, s3_path, credentials.access_key, credentials.secret_key, 'auto')
        
        
        self.log.info(f"Running query : {copy_query}")
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        redshift_hook.run(copy_query)
        self.log.info(f"Successfully staged: {self.table_name} from S3 bucket to Redshift.")

