import tagManager
import utils.fileManager as fileManager
import logging

class start_talk(object):
    def mainMenu(self):
        logging.info("Starting talk to user")
        menu = "\n ### MAIN MENU ### \n What task do you wish to perform? \n 1 = input new data \n 2 = search existing data by tag: \n Enter a number: "
        answer = input(menu)
        return answer
    
    def inputHandler(self):
        answer = self.mainMenu()
        if answer == "1":
            logging.debug("MAIN MENU: user has entered option 1")

            i = input(">> Enter question number: ")
            q = f"q{i}"
            questionsAndTags = fileManager.loadDataFrom("./questionsAndTags.yaml")
            # dealing with existing questions
            if q in questionsAndTags:
                questionValue = questionsAndTags.items(q)
                print("Current values:")
                print(f">> Question {q}: {questionValue}")
                i = input("OVERWRITTE current values (Y/n)? ")
                # OVERWRITE MODE ON
                if i == "Y" or i == "y":
                    i = input("ATENTION: All current values will be lost. Press Enter to cancel or insert 'go' to proceed: ")
                    if i == "Y" or i == "y":
                        t = input(f"enter TAGS for question {q} (separeted, by, comma): ")
                        tagManager.inputTags(question=q, tags=t, overwritte=True)
                    else:
                        pass
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

    