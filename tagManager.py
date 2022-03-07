import fileManager
import logSet
import logging

logSet.enableLog()

def inputTags(question=dict, tags=list):
    logging.info(" #### iniciating inputTags ###")
    questionsAndTags = fileManager.loadDataFrom("./questionsAndTags.yaml")
    logging.info("previous questionsAndTags:")
    logging.info(questionsAndTags)

    #Add only new tags if question alerady exists
    if question in questionsAndTags:
        qt = questionsAndTags[question]
        for t in tags:
            if t in qt:
                pass
            else:
                questionsAndTags[question].append(t)
    else:
        questionsAndTags[question] = tags

    
    logging.info("final questionsAndTags:")
    logging.info(questionsAndTags)

    fileManager.updateFile(questionsAndTags, "./questionsAndTags.yaml")

    questionsAndTags = fileManager.loadDataFrom("./questionsAndTags.yaml")
    logging.info("file looks like:")
    logging.info(questionsAndTags)

    return questionsAndTags

inputTags(question="q54", tags=["Lambda", "CloudWatch"])




# def findQuestions(tags={}):
#     a = 