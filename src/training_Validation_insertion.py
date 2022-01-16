from datetime import datetime
from src.TrainingModel_validation.Raw_Data_Validation import Raw_Data_Validation
from src.DataTypeValidation_Insertion_Training.DataTypeValidation import dbOperation
from src.DataTransfrom_Training.DataTransfromation import dataTransform
from src.application_logging.logger import App_logger

class train_validation:
    def __init__(self,path):
        self.raw_data= Raw_Data_Validation(path)
        self.dataTransform = dataTransform()
        self.dbOperation = dbOperation()
        self.logfile = "Logs/Training_Logs/Training_Main_Log.txt"
        self.logger = App_logger()

    def training_validaiton(self):
        try:
            self.logger.logger(self.logfile,"Start of Validation on files for Prediction")
            LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names, noofcolumns = self.raw_data.valuesFromSchema()
            regex = self.raw_data.manualRegexCreation()
            self.raw_data.validationFileNameRaw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)
            self.raw_data.validateColumnLength(noofcolumns)
            self.raw_data.validateMissingValuesInWholeColumn()
            self.logger.logger(self.logfile,"Raw Data validation completed !!!")
            self.logger.logger(self.logfile,"Starting Data transformation !!! ")
            self.dataTransform.addQuotesToStringValuesInColumn()
            self.logger.logger(self.logfile,"Data transformation is completed")
            self.logger.logger(self.logfile,
                               "Creating Training_Database and tables on the basis of given schema!!!")
            self.dbOperation.createTableDB("Training",column_names)
            self.logger.logger(self.logfile,"Table creation completed")
            self.logger.logger(self.logfile,"Insertion of Data into Table started!!!!")
            self.dbOperation.insertIntoTableGoodData("Training")
            self.logger.logger(self.logfile, "Insertion in Table completed!!!")
            self.logger.logger(self.logfile, "Deleting Good Data Folder!!!")

            self.raw_data.deleteExisitingGoodTrainingFolder()
            self.logger.logger(self.logfile, "Good_Data folder deleted!!!")
            self.logger.logger(self.logfile, "Moving bad files to Archive and deleting Bad_Data folder!!!")

            self.raw_data.moveBadFilesToArchiveBad()
            self.logger.logger(self.logfile, "Bad files moved to archive!! Bad folder Deleted!!")
            self.logger.logger(self.logfile, "Validation Operation completed!!")
            self.logger.logger(self.logfile, "Extracting csv file from table")

            self.dbOperation.selectingDatafromtableintocsv('Training')

        except Exception as e:
            raise e