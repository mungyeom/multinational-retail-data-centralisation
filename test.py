class DatabaseConnector:
    
    def __init__(self,filename):
        self.filename = filename
        self.cred = self.read_db_creds()
        self.engine = self.init_db_engine()

    def read_db_creds(self):
       with open(self.filename,'r') as file:
            service_prime = yaml.safe_load(file)
            return service_prime

    def init_db_engine(self):
        service_prime = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        USER = service_prime['RDS_USER']
        PASSWORD = service_prime['RDS_PASSWORD']
        HOST = service_prime['RDS_HOST']
        PORT = service_prime['RDS_PORT']
        DATABASE = service_prime['RDS_DATABASE']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine.connect
        return engine
    
class DataExtractor(DatabaseConnector):

    def __init__(self,filename):
        super().__init__(filename)
        self.list_table = self.list_db_tables()
        self.read_table = self.read_rds_table()

    def list_db_tables(self):
        inspector = inspect(self.engine)
        list_table = inspector.get_table_names()
        return list_table
    
    def read_rds_table(self):
        self.list_table
        legacy_store_details = pd.read_sql_table('legacy_store_details', self.engine)
        legacy_users = pd.read_sql_table('legacy_users', self.engine)
        orders_table = pd.read_sql_table('orders_table', self.engine)
        return legacy_store_details,legacy_users,orders_table