from user_interface import talkToUser
import logSet
import logging

logSet.enableLog(".", "tagmanager2")
logging.info(' ++++++++++++++++++++++++++++++++++ tagmanager initiated ++++++++++++++++++++++++++++++++++')

talkToUser.start_talk()