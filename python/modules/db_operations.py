# External libraries
from logging import exception as log_exception

# Internal modules
from modules.setup import Config
from modules.file_processing import FileProcessing

class DbOperations():
    def __init__(self, configuration: Config) -> None:
        # During the instance creation, only the connection string values are created
        self.db_host = configuration.db_host
        self.db_name = configuration.db_name
        self.db_user = configuration.db_user
        self.db_pass = configuration.db_pwd
   
    def connect_db(self):
        from sqlalchemy import create_engine
        
        try:
            # Creating the connection engine
            engine = create_engine(f"mysql+pymysql://{self.db_user}:{self.db_pass}@{self.db_host}:3306/{self.db_name}")
        except Exception as e:
            log_exception(e)
        return engine
    
    def export_data(self, file_process: FileProcessing) -> None:
        keys = list(file_process.__dict__.keys())

        # We obtain a list of all the instantiated dataframes for the process
        dfs = [i for i in keys if i.startswith('df_')]
        df_objects = [getattr(file_process, i) for i in dfs]

        # Additionally, we generate the table names by removing the dataframe prefix
        df_names = [str(i).replace('df_', '').lower() for i in keys if i.startswith('df_')]

        # We generate the DB connection
        engine = self.connect_db()

        # We export each dataframe to its corresponding MySQL table
        [df_objects[index].to_sql(df_names[index], con=engine, if_exists='append', index=False) for index, value in enumerate(df_names)]