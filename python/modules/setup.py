from logging import exception as log_exception, info as log_info

class Config():
    def __init__(self, filename: str):
        from os import path
        
        self.filename = filename
        if not path.exists(self.filename):
            self.read(True)
            self.set_up()
        else:
            self.read()

    def read(self, blank_config = False):
        # External libraries:
        from os import getenv
        from ruamel.yaml import round_trip_load

        if blank_config:
            filename = 'config_template.yaml'
        else:
            filename = self.filename

        try:
            with open(filename, 'r') as f:
                self.cfg = round_trip_load(f)

            log_info('Configuration read successfully')

            if not blank_config:                
                # DB Connection Info
                self.db_host    = self.cfg['database']['host']
                self.db_name    = self.cfg['database']['name']
                self.db_user    = self.cfg['database']['user']

                # Reading the password from the configuration file, or the indicated environment variable
                if not self.cfg['database']['plain_pwd']:
                    pwd_env_var = self.cfg['database']['password_var']
                    self.db_pwd = getenv(pwd_env_var)
                else:
                    self.db_pwd = self.cfg['database']['password_val']


        except Exception as e:
            log_exception(e)

    def update(self, cfg: str):
        from ruamel.yaml import round_trip_dump

        with open(self.filename, "w") as f:
            round_trip_dump(cfg, f)

    def set_up(self):
        import pwinput
        from distutils.util import strtobool

        self.cfg['general']['log_file'] = input('Name of the log file:\t')
        self.cfg['general']['process_file'] = input('Name of the file to process:\t')
        self.cfg['database']['name']    = input('Name of the target database:\t')
        self.cfg['database']['user']    = input('Username for this process:\t')
        self.cfg['database']['plain_pwd']   = strtobool(input('Is the password stored in an environment variable?\t'))

        if self.cfg['database']['plain_pwd']:
            self.cfg['database']['password_var'] = input('Name of the environment variable:\t')
        else:
            self.cfg['database']['password_val'] = pwinput.pwinput(prompt='Password:\t')
        
        self.update(self.cfg)