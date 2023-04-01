# Internal modules
from modules.setup import Config
from modules.db_operations import DbOperations
from modules.file_processing import FileProcessing

# External libraries
from argparse import ArgumentParser
from logging import basicConfig as log_config, exception as log_exception, INFO

def main():
    # We can pass multiple configurations to the script, which would serve to recycle the structure of it
    arg_parser = ArgumentParser()
    arg_parser.add_argument('-c', '--cfg_file', nargs='?', const='config.yaml', default='config.yaml', type=str, help='Name of the configuration file to read')
    args = arg_parser.parse_args()

    configuration = Config(args.cfg_file)
    
    # Logging format and configuration file
    log_format = '%(asctime)s - %(message)s'
    log_config(filename=configuration.cfg['general']['log_file'], format=log_format, level=INFO, force=True)

    # Generating the process instances
    fp = FileProcessing(configuration)
    db_ops = DbOperations(configuration)

    # Exporting the data.
    db_ops.export_data(fp)

if __name__ == "__main__":
   main()