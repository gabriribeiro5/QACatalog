import fileManager
import logSet
import logging

def inputTags(question=dict, tags=list, overwritte:bool=False):
    logging.info(" #### iniciating inputTags ###")
    questionsAndTags = fileManager.loadDataFrom("./questionsAndTags.yaml")
    logging.info("previous questionsAndTags:")
    logging.info(questionsAndTags)

    # cases to ignore existing values
    if question not in questionsAndTags or overwritte:
        questionsAndTags[question] = tags
    #Add only new tags if question alerady exists
    else:
        qt = questionsAndTags[question]
        for t in tags:
            if t in qt:
                pass
            else:
                questionsAndTags[question].append(t)
        

    
    logging.info("final questionsAndTags:")
    logging.info(questionsAndTags)

    fileManager.updateFile(questionsAndTags, "./questionsAndTags.yaml")

    questionsAndTags = fileManager.loadDataFrom("./questionsAndTags.yaml")
    logging.info("file looks like:")
    logging.info(questionsAndTags)

    return questionsAndTags

inputTags(question="q54", tags=["Yeh! Science bitch!"])




# def findQuestions(tags={}):
#     a = 