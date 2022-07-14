import utils.yamlManager as yamlManager
import utils.logSet as logSet
import logging
import datetime

def inputTags(question=dict, tags=list, overwritte:bool=False):
    now = datetime.datetime.today()
    logging.info(f" \n {now} \n #### iniciating inputTags ####")
    # logging.warning(f"Overwritte mode: {overwritte} \n")

    questionsAndTags = yamlManager.loadDataFrom("./questionsAndTags.yaml")

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
    
    yamlManager.updateFile(questionsAndTags, "./questionsAndTags.yaml")
    logging.info("file updated")
    return questionsAndTags


def findQuestions(tags:dict):
    """Find all questions containing the given words
    Response will be ordered by questions that have:
    1. Exactly the same sequence of words
    2. The given words in the same tag, no matter the sequence
    3. The given words at any tag
    4. Most of the given words
    5. Any of the given words
    """
    now = datetime.datetime.today()
    logging.info(f" \n {now} \n #### searching questions by tags ####")

    questionsAndTags = yamlManager.loadDataFrom("./questionsAndTags.yaml")
    questionsList:dict = {}

    for t in tags:
        for k, v in questionsAndTags.items():
            if t in v:
                questionsList[k] = v
    
    return questionsList