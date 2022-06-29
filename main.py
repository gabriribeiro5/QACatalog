# Dealing with ImportError: attempted relative import with no known parent package
import sys
sys.path.append('.')

# Setup logging configuration
import utils.logSet as logSet
import logging
logSet.enableLog("logs", "QACatalog")

# Aplication libs
from user_interface import talkToUser

appName = "QACatalog"
logging.info(f' ++++++++++++++++++++++++++++++++++ {appName} INITIATED  ++++++++++++++++++++++++++++++++++')

a = talkToUser.start_talk()
a.mainMenu()