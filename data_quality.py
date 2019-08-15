from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.args= args
        self.kwargs = kwargs

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Data quality check on users table')
        records = redshift_hook.get_records(self.kwargs["params"]["users_data_check"])
        self.log.info(f'Users table records {records}')
        if len(records) < 0 or len(records[0]) < 0:
            raise ValueError(f"Data quality check failed. users table returned no results")
        num_records = records[0][0]
        if num_records > self.kwargs["params"]["users_data_result"]:
            raise ValueError(f"Data quality check failed. users table contained NULL values")
        logging.info(f"Data quality on table users check passed with {records[0][0]} records")
        
        self.log.info('Data quality check on songs table')
        records2 = redshift_hook.get_records(self.kwargs["params"]["songs_data_check"])
        self.log.info(f'songs table records {records2}')
        if len(records2) < 0 or len(records2[0]) < 0:
            raise ValueError(f"Data quality check failed. songs table returned no results")
        num_records2 = records2[0][0]
        if num_records2 > self.kwargs["params"]["songs_data_result"]:
            raise ValueError(f"Data quality check failed. songs table contained NULL values")
        logging.info(f"Data quality on table songs check passed with {records2[0][0]} records")
        
        self.log.info('Data quality check on artists table')
        records3 = redshift_hook.get_records(self.kwargs["params"]["artists_data_check"])
        self.log.info(f'artists table records {records3}')
        if len(records3) < 0 or len(records3[0]) < 0:
            raise ValueError(f"Data quality check failed. artists table returned no results")
        num_records3 = records3[0][0]
        if num_records3 > self.kwargs["params"]["artists_data_result"]:
            raise ValueError(f"Data quality check failed. artists table contained NULL values")
        logging.info(f"Data quality on table artists check passed with {records3[0][0]} records")
        
        self.log.info('Data quality check on time table')
        records4 = redshift_hook.get_records(self.kwargs["params"]["time_data_check"])
        self.log.info(f'time table records {records4}')
        if len(records4) < 0 or len(records4[0]) < 0:
            raise ValueError(f"Data quality check failed. time table returned no results")
        num_records4 = records4[0][0]
        if num_records4 > self.kwargs["params"]["time_data_result"]:
            raise ValueError(f"Data quality check failed. time table contained NULL values")
        logging.info(f"Data quality on table time check passed with {records4[0][0]} records")
        
        
        
        
        
        