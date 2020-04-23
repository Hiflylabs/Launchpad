import logging
import logging.config
import os

def create_logger(name, logname, logdir = '/log/'):
    """The logger constructor, called by every Pipe at initation.
    
    Args:
        name (str): A unique name of the Pipe, which is included in the log message.
        logname (str): The name of the log file where the logs will be written to.

    Retuns:
        logging.Logger
    
    """

    #Create log folder if it does not exist
    if not os.path.exists(logdir):
        os.makedirs(logdir)

    #Add the name of the Pipe to the log message
    extra = {'pipename':name}

    #Logger config
    config = {
        'disable_existing_loggers': False,
        'version': 1,
        'formatters': {
            'default': {
                'format' : '%(asctime)s - %(pipename)s - %(levelname)s - %(message)s'
            },
        },
        'handlers': {
            'console': {
                'level': 'INFO',
                'formatter': 'default',
                'class': 'logging.StreamHandler',
            },
            'file' : {
                'level': 'DEBUG',
                'formatter' : 'default',
                'class': 'logging.handlers.TimedRotatingFileHandler',
                'filename' : logdir + logname + '.log'
            }
        },
        'loggers': {
            logname: {
                'handlers': ['console', 'file'],
                'level' : 'DEBUG'
            },
        },
    }
    

    #Set logging config
    logging.config.dictConfig(config)
    logger = logging.getLogger(logname)
    logger = logging.LoggerAdapter(logger, extra)

    return logger


