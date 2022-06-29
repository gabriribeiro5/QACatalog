# Dealing with ImportError: attempted relative import with no known parent package
import sys
sys.path.append('.')

# Setup logging configuration
import utils.logSet as logSet
import logging
logSet.enableLog("logs", "QACatalog")

# Aplication libs
from controller import activate

appName = "QACatalog"
logging.info(f' ++++++++++++++++++++++++++++++++++ {appName} INITIATED  ++++++++++++++++++++++++++++++++++')

#Activate Controller
activate.Manager(appName)