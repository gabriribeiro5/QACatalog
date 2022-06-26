# Dealing with ImportError: attempted relative import with no known parent package
import sys
sys.path.append('.')

import utils.logSet as logSet
import logging

logSet.enableLog("tests", "logTester")

logging.debug('logTester starts here')
logging.info('This message should go to the log file')
logging.warning('And this, too')
logging.error('And non-ASCII stuff, too, like Øresund and Malmö')

anyVar = 'xablau'
anotherVar = 'bazinga'
logging.info('Variable 1 (%s) and variable 2 (%s) come from python code', anyVar, anotherVar)