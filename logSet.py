# Dealing with ImportError: attempted relative import with no known parent package
import sys
sys.path.append('.')

# logging setup
from scripts.log import fileManager
import os
import logging

import inspect

def enableLog(dirName:str=None, appName:str="logfile"):
    """

    dirName: glue-a, glue-b, lambda, murabei
    appName: nome do arquivo atual ou aplicacao que estã ativando enableLog()

    """

    # fileName = fileManager.loadConfigFile()
    # logFilePathAndName = os.path.join(os.environ['LOG_DIR'], fileName['logFile'])

    #remove .py from appName
    if ".py" in appName:
        appName = appName[0:(len(appName)-3)]


    logFilePathAndName = f"./{dirName}/{appName}.log"

    logging.basicConfig(filename=logFilePathAndName,
                         format='%(levelname)s[%(asctime)s] - %(module)s: %(message)s',
                         datefmt='%Y/%m/%d %I:%M:%S %p',
                         filemode='a',
                         level=logging.DEBUG)   

    # New paragraph
    with open(logFilePathAndName, 'a') as l:
        # find the calling module
        frm = inspect.stack()[1] # the call's frame: filepath, code line, etc
        mod = inspect.getmodule(frm[0])
        callingModule = inspect.getmodulename(mod.__file__)

        # append first line of the process
        l.write(f"\n -- Logger enabled by {callingModule} --\n")
    
    logging.info(' This is a message from logSet \n ')
