import tagManager
import fileManager
import logSet
import logging
import datetime

logSet.enableLog()

answer = input("### MENU ### \n 1. input \n 2. search by tag: \n >>")

# input handler
if answer == "1":
    i = input(">> Enter question number: ")
    q = f"q{i}"
    questionsAndTags = fileManager.loadDataFrom("./questionsAndTags.yaml")
    # dealing with existing questions
    if q in questionsAndTags:
        questionValue = questionsAndTags.items(q)
        print(f">> Question {q}: {questionValue}")
        i = input("OVERWRITTE current values (Y/n)? ")
        # OVERWRITE MODE ON
        if i == "Y" or i == "y":
            i = input("ATENTION: All current values will be lost if you proceed. Proceed(Y/n)? ")
            if i == "Y" or i == "y":
                t = input(f"TAGS for question {q} (separeted, by, comma): ")
                tagManager.inputTags(question=q, tags=t, overwritte=True)
        # OVERWRITE MODE OFF
        else:
            t = input(f"TAGS for question {q} (separeted, by, comma): \n >>")
            tagManager.inputTags(question=q, tags=t)
    # dealing with new questions
    else:
        i = input("Question number not found. \n 1. Continue to add new question \n 2. Enter new question number \n >>")
        if i == "1":
            t = input(f"TAGS for question {q} (separeted, by, comma): \n >>")
            tagManager.inputTags(question=q, tags=t)


        # tagManager.inputTags(question=q, tags=t)

    i = input("Enter, tags, separeted, by coma:")
    # tagManager.inputTags(question=q, tags=t)