import logging
import pals_helpers
import os

def initialize_logger(__name__, config: str):

    config = pals_helpers.load_config(config).get('logging_info')    
    log_directory = os.path.join(os.getcwd(), config.get('filepath')) 
    log_filename = os.path.join(log_directory, config.get('filename'))
    
    if not os.path.exists(log_directory):
        try:
            os.makedirs(log_directory)
        except:
            raise OSError(f'Unable to create directory {log_directory}')

    try:
        logging.basicConfig(filename=log_filename,
                            filemode=config.get('filemode'),
                            format=config.get('format'),
                            datefmt=config.get('dateformat'),
                            level=__get_logging_level(config.get('level')))
    except:
        raise Exception('Error occured when initializing logger. Check the config.json for typos')
    
    return logging.getLogger(__name__)

def __get_logging_level (level):

    if level in ['Info', 'INFO', 'info']:
        level = logging.INFO
    if level in ['Debug', 'DEBUG', 'debug']:
        level = logging.DEBUG
    if level in ['Error', 'ERROR', 'error']:
        level = logging.ERROR
    if level in ['Warning', 'WARNING', 'warning']:
        level = logging.WARNING

    return level