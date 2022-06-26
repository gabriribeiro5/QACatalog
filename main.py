# Dealing with ImportError: attempted relative import with no known parent package
import sys
sys.path.append('.')

from user_interface import talkToUser
import utils.logSet as logSet
import logging

logSet.enableLog(".", "tagManager")
print("d")
logging.info(' ++++++++++++++++++++++++++++++++++ tagmanager initiated ++++++++++++++++++++++++++++++++++')

a = talkToUser.start_talk()
a.mainMenu()
print("e")