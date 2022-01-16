from datetime import datetime
import os
from os import listdir
import re
import json
import shutil
import pandas as pd
from src.application_logging.logger import App_logger
import src.Generic.Utility as utility

class Raw_Data_Validation:
    def __init__(self,path):
        self.Batch_directory =path
        self.schema_path = utility.read_yamlfile(os.path.join("config","params.yaml"))['config']['training_schema']
        self.logger = App_logger()
        self.good_raw = utility.read_yamlfile(os.path.join("config","params.yaml"))['training_filepath']['good_raw']
        self.bad_raw= utility.read_yamlfile(os.path.join("config","params.yaml"))['training_filepath']['bad_raw']

    def valuesFromSchema(self):
        filepath = "Logs/Training_Logs/valuesfromSchemaValidationLog.txt"
        try:
            with open(self.schema_path,'r') as f:
                dic = json.load(f)
                f.close()
            pattern =dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']
            # add logic to create folders dynamically
            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.logger(filepath,message)
        except ValueError:
            self.logger.logger(filepath, "Value Error: Value not found inside training_schema.json")
            raise ValueError
        except KeyError:
            self.logger.logger(filepath,"KeyError: key value error incorrect in training_schema.json")
            raise KeyError
        except Exception as e:
            self.logger.logger(filepath, "Exception: occured at training_schema.json, Exception is "+ str(e))
            raise e

        return LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names,NumberofColumns


    def manualRegexCreation(self):
        regex = "['phising']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):
        path = "Training_Raw_files_validated/"
        goodpath = os.path.join(path,"Good_Raw/")
        badpath = os.path.join(path,"Bad_Raw")
        training_logs = "Logs/Training_Logs/GeneralLog.txt"
        try:
            utility.createDirectory(goodpath)
            utility.createDirectory(badpath)
        except OSError as e:
            self.logger.logger(training_logs,"Error while creating Directory "+str(e))
            raise OSError


    def deleteExisitingGoodTrainingFolder(self):
        path = "Training_Raw_files_validated/"
        training_logs = "Logs/Training_Logs/GeneralLog.txt"
        try:
            utility.deleteDiretory(os.path.join(path,"Good_Raw/"))
            self.logger.logger(training_logs,"Good_Raw directory deleted successfully.")
        except OSError as e:
            self.logger.logger(training_logs,"Error while deleting Good_Raw Directory "+str(e))
            raise OSError

    def deleteExisitingBadTrainingFolder(self):
        path = "Training_Raw_files_validated/"
        training_logs = "Logs/Training_Logs/GeneralLog.txt"
        try:
            utility.deleteDiretory(os.path.join(path,"Bad_Raw/"))
            self.logger.logger(training_logs,"Bad_Raw directory deleted successfully.")
        except OSError as e:
            self.logger.logger(training_logs,"Error while deleting Bad_Raw Directory "+str(e))
            raise OSError

    def moveBadFilesToArchiveBad(self):
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H:%M:%S")
        source = "Training_Raw_files_validated/Bad_Raw/"
        f = "Logs/Training_Logs/GeneralLog.txt"
        try:
            if os.path.isdir(source):
                path = "TrainingArchiveBadData"
                utility.createDirectory(path)
            dest =os.path.join('TrainingArchiveBadData/BadData_'+str(date)+"_"+str(time)).replace(":","")
            if not os.path.isdir(dest):
                os.makedirs(dest)
            files = os.listdir(source)
            for f in files:
                if f not in os.listdir(dest):
                    shutil.move(source,dest)
            self.logger.logger(f,"Bad files moved to archive")
            path = 'Training_Raw_files_validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
            self.logger.logger(f, "Bad Raw Data Folder Deleted successfully!!")
        except Exception as e:
            self.logger.logger(f,"Error while moving bad files to archive:: %s" % e)
            raise e

    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        f= "Logs/Training_Logs/nameValidationLog.txt"
        self.deleteExisitingBadTrainingFolder()
        self.deleteExisitingGoodTrainingFolder()
        onlyfiles = [files for files in listdir(self.Batch_directory)]
        try:
            # create new directories
            self.createDirectoryForGoodBadRawData()
            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy("src/Training_Batch_Files/" + filename, "Training_Raw_files_validated/Good_Raw")
                            self.logger.logger(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)
                        else:
                            shutil.copy("src/Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                            self.logger.logger(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy("src/Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                        self.logger.logger(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("src/Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.logger(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

        except Exception as e:
            self.logger.logger(f,"Error occured while validating fileName "+str(e))
            raise e

    def validateColumnLength(self,NumberofColumns):
        f= "Logs/Training_Logs/columnValidationLog.txt"
        try:
            self.logger.logger(f,"Column length validation Started!!")
            for file in listdir(self.good_raw):
                csv = pd.read_csv(self.good_raw+"//"+file)
                if csv.shape[1]==NumberofColumns:
                    pass
                else:
                    shutil.move(self.good_raw+"//"+file,self.bad_raw)
                    self.logger.logger(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            self.logger.logger(f,"Column Length Validation Completed!!")
        except Exception as e:
            self.logger.logger(f,"Error occurred :: "+str(e))
            raise e

    def validateMissingValuesInWholeColumn(self):
        f= "Logs/Training_Logs/missingValuesInColumn.txt"
        try:
            self.logger.logger(f,"Missing values validation is started!!!")
            for file in listdir(self.good_raw):
                csv = pd.read_csv(self.good_raw+"//"+file)
                count =0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count +=1
                        shutil.move(self.good_raw+"//"+file,self.bad_raw)
                        break
                    if count ==0:
                        csv.to_csv(self.good_raw+"//"+file,index=None,header=True)
        except Exception as e:
            self.logger.logger(f,"Error occurred ::" +str(e))
            raise e




