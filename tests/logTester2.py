# Dealing with ImportError: attempted relative import with no known parent package
import sys
sys.path.append('.')

from scripts.log import logSet
import logging

logSet.enableLog("tests", "logTester2")

logging.debug('logTester starts here')
logging.info('This message should go to the log file')
logging.warning('And this, too')
logging.error('And non-ASCII stuff, too, like Øresund and Malmö')