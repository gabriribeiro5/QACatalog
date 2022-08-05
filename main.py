# Dealing with ImportError: attempted relative import with no known parent package
import sys
sys.path.append('.')

# Setup logging configuration
import utils.logSet as logSet
import logging
logSet.enableLog("logs", "QACatalog")

# Aplication libs
from controller.activate import Manager_Work_Flow
from utils import yamlManager

def mainFunc():
    try:
        configData = yamlManager.loadDataFrom("config.yaml")
        appName = configData["appName"]
    except:
        msg = "Couldn't load config.yaml'"
        raise(msg)

    logging.info(f' ++++++++++++++++++++++++++++++++++ {appName} INITIATED  ++++++++++++++++++++++++++++++++++')

    #Activate Controller
    manager = Manager_Work_Flow(configData)
    manager.runTasks()

    logging.info(f' ---------------------------------- {appName} TERMINATED  ---------------------------------')


if __name__ == "__main__":
    mainFunc()