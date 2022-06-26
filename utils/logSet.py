# Dealing with ImportError: attempted relative import with no known parent package
import pdb
import sys
sys.path.append('.')

# logging setup
# import utils.fileManager as fileManager
# import os
import logging
import inspect

def enableLog(dirName:str=".", logFileName:str="logfile"):
    """

    dirName: where must the lofile be saved?
    logFileName: what is the logfile name (usually the application name)?

    """

    # fileName = fileManager.loadConfigFile()
    # logFilePathAndName = os.path.join(os.environ['LOG_DIR'], fileName['logFile'])

    #remove .py from appName
    if ".py" in logFileName:
        logFileName = logFileName[0:(len(logFileName)-3)]

    print(logFileName)
    pdb.set_trace()
    fileName = f"{logFileName}.log"
    pdb.set_trace()
    logging.basicConfig(filename="test.log", encoding='utf-8', level=logging.DEBUG)
    logging.basicConfig(filename=fileName, encoding='utf-8', level=logging.DEBUG)
    pdb.set_trace()
    logging.debug('This message should go to the log file')
    pdb.set_trace()
    logging.info('So should this')
    logging.warning('And this, too')
    logging.error('And non-ASCII stuff, too, like Øresund and Malmö')

    pdb.set_trace()
    logFilePathAndName = f"{dirName}/{logFileName}.log"
    pdb.set_trace()
    fileName = f"{logFileName}.log"
    pdb.set_trace()
    print(logFilePathAndName)
    logging.basicConfig(filename=logFilePathAndName,
                         format='%(levelname)s[%(asctime)s] - %(module)s: %(message)s',
                         datefmt='%Y/%m/%d %I:%M:%S %p',
                         filemode='a',
                         level=logging.debug)
    pdb.set_trace()
    logging.debug('logging lib test for debug level is ok, running into level')
    logging.info('logging lib test for info level is ok, running into warining level')
    print("b")
    # New paragraph
    with open(logFilePathAndName, 'a') as l:
        # find the calling module
        frm = inspect.stack()[1] # the call's frame: filepath, code line, etc
        mod = inspect.getmodule(frm[0])
        callingModule = inspect.getmodulename(mod.__file__)

        # append first line of the process
        l.write(f"\n -- Logger enabled by {callingModule}.py --\n")

    logging.debug('logging lib test for debug level is ok, running into level')
    logging.info('logging lib test for info level is ok, running into warining level')
    print("c")
    # logging.warning('logging lib test for warning level is ok, running into warning level')
    # logging.error('logging lib test for error level is ok, running into critical level')
    # logging.critical('logging lib test for critical level is ok, all levels have been tested')
