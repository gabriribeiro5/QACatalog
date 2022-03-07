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

    logging.basicConfig(level=logging.DEBUG)
    logging.FileHandler(filename=logFilePathAndName)
