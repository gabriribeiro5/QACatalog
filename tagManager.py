import fileManager
import logSet
import logging

logSet.enableLog()

def inputTags(question=dict, tags=list):
    logging.info(" #### iniciating inputTags ###")
    questionsAndTags = fileManager.loadDataFrom("./QuestionsAndTags.yaml")
    logging.info("previous questionsAndTags:")
    logging.info(questionsAndTags)

    if question in questionsAndTags:
        # questionsAndTags[question].update(tags)
        i = questionsAndTags.index(question)
        questionsAndTags.remove(question)
    else:
        questionsAndTags[question] = tags

    
    logging.info("final questionsAndTags:")
    logging.info(questionsAndTags)

    fileManager.updateFile(questionsAndTags, "./QuestionsAndTags.yaml")

    questionsAndTags = fileManager.loadDataFrom("./QuestionsAndTags.yaml")
    logging.info("file looks like:")
    logging.info(questionsAndTags)

    return questionsAndTags

inputTags(question="q54", tags=["Lambda", "CloudWatch"])




# def findQuestions(tags={}):
#     a = 