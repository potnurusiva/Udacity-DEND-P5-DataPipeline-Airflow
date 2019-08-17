from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table = "",
                 redshift_conn_id="",
                 create_table_sql = "",
                 insert_table_sql = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.create_table_sql = create_table_sql
        self.insert_table_sql = insert_table_sql

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Deleting the table if already exists")
        redshift.run("DROP TABLE IF EXISTS {}".format(self.table))
        self.log.info(f"creating fact table {self.table}")
        self.log.info(f"creating fact table query {self.create_table_sql}")
        redshift.run(self.create_table_sql)
        self.log.info("Clearing data from fact Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Inserting data from staging tables to fact table")
        redshift.run(self.insert_table_sql)
