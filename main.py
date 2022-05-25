from user_interface import talkToUser
import logSet
import logging

logSet.enableLog(".", "tagManager")
print("d")
logging.info(' ++++++++++++++++++++++++++++++++++ tagmanager initiated ++++++++++++++++++++++++++++++++++')

a = talkToUser.start_talk()
a.mainMenu()
print("e")