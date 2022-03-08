import fileManager
import logSet
import logging
import datetime

def inputTags(question=dict, tags=list, overwritte:bool=False):
    now = datetime.datetime.today()
    logging.info(f" \n {now} \n #### iniciating inputTags ####")
    logging.warning(f"Overwritte mode: {overwritte} \n")

    questionsAndTags = fileManager.loadDataFrom("./questionsAndTags.yaml")

    # Cases to ignore existing values
    if question not in questionsAndTags or overwritte:
        logging.info(f" {now} \n adding new values or overwritting existing ones")
        questionsAndTags[question] = tags
    # Add only new tags if question alerady exists
    else:
        logging.info("appending new values only")
        qt = questionsAndTags[question]
        for t in tags:
            if t in qt:
                pass
            else:
                questionsAndTags[question].append(t)
    
    fileManager.updateFile(questionsAndTags, "./questionsAndTags.yaml")
    logging.info("file updated")
    return questionsAndTags

inputTags(question="q54", tags=["Yeh! Science bitch!"], overwritte=True)


def findQuestions(tags={}):
    now = datetime.datetime.today()
    logging.info(f" \n {now} \n #### searching questions by tags ####")

    questionsAndTags = fileManager.loadDataFrom("./questionsAndTags.yaml")
    questionsList:dict = {}

    for t in tags:
        for k, v in questionsAndTags.items():
            if t in v:
                questionsList[k] = v
    
    return questionsList

questionsList = findQuestions(tags=["Lambda", "CloudWatch"])
print(f"questionsList: {questionsList}")