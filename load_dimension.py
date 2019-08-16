from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table = "",
                 redshift_conn_id="",
                 create_table_sql = "",
                 insert_table_sql = "", 
                 load_type = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.create_table_sql = create_table_sql
        self.insert_table_sql = insert_table_sql
        self.load_type = load_type

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.load_type == "delete-load":
            self.log.info("Deleting the table if already exists")
            redshift.run("DROP TABLE IF EXISTS {}".format(self.table))
            
        self.log.info(f"creating dimension table {self.table}")
        self.log.info(f"creating dimension table query {self.create_table_sql}")
        redshift.run(self.create_table_sql)
        if self.load_type == "delete-load":
            self.log.info("Clearing data from dimension Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))
            
        self.log.info("Inserting data from staging tables to dimension tables")
        redshift.run(self.insert_table_sql)
        
        
        
