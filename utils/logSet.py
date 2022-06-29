# Dealing with ImportError: attempted relative import with no known parent package
import pdb
import sys
sys.path.append('.')

# logging setup
# import utils.fileManager as fileManager
# import os
import logging
import inspect

def _addLine(FilePathAndName, newLine):
    with open(FilePathAndName, 'a') as l:
        # append first line of the process
        l.write(newLine)

def enableLog(dirName:str="log", logFileName:str="logfile"):
    """

    dirName: where must the lofile be saved?
    logFileName: what is the logfile name (usually the application name)?

    """

    # fileName = fileManager.loadConfigFile()
    # logFilePathAndName = os.path.join(os.environ['LOG_DIR'], fileName['logFile'])

    #remove .py from appName
    if ".py" in logFileName:
        logFileName = logFileName.split(".")[0]

    logFilePathAndName = f"{dirName}/{logFileName}.log"
        
    logging.basicConfig(filename=logFilePathAndName,
                         format='%(levelname)s[%(asctime)s] - %(module)s: %(message)s',
                         datefmt='%Y/%m/%d %I:%M:%S %p',
                         filemode='a',
                         level=logging.DEBUG)

    # find the calling module
    frm = inspect.stack()[1] # the call's frame: filepath, code line, etc
    mod = inspect.getmodule(frm[0])
    callingModule = inspect.getmodulename(mod.__file__)
    newLine = f"\n -- Logger enabled by {callingModule}.py --\n"
    _addLine(logFilePathAndName, newLine)
    
    logging.debug('logging test for DEBUG level [check], next line must be INFO level')
    logging.info('logging test for INFO level [check], next line must be WARNING level')
    logging.warning('logging test for WARNING level [check], next line must be ERROR level')
    logging.error('logging test for ERROR level [check], next line must be CRITICAL level')
    logging.critical('logging test for ERROR level [check], all levels have been tested')

    newLine = "\n"
    _addLine(logFilePathAndName, newLine)
