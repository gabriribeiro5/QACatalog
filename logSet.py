# Dealing with ImportError: attempted relative import with no known parent package
import sys
sys.path.append('.')

# logging setup
import fileManager
import os
import logging

def enableLog():
    # fileName = fileManager.loadConfigFile()
    # logFilePathAndName = os.path.join(os.environ['TAG_MANAGER_LOG_DIR'], fileName['logFile'])
    logFilePathAndName = "./tagManager.log"

    logging.basicConfig(filename=logFilePathAndName, level=logging.DEBUG)
    # logging.FileHandler(filename=logFilePathAndName)

    logging.debug('This message should go to the log file')
    logging.info('So should this')
    logging.warning('And this, too')
    logging.error('And non-ASCII stuff, too, like Øresund and Malmö')

