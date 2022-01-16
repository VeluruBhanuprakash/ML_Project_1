from datetime import datetime
import os
from os import listdir
from src.application_logging.logger import App_logger
import pandas as pd
from src.Generic.Utility import read_yamlfile

class dataTransform:
    def __init__(self):
        self.goodDataPath = read_yamlfile(os.path.join("config","params.yaml"))['training_filepath']['good_raw']
        self.logger = App_logger()

    def addQuotesToStringValuesInColumn(self):
        logfile = "Logs/Training_Logs/addQuotesToStringValuesInColumn.txt"
        try:
            onlyfiles= [f for f in listdir(self.goodDataPath)]
            for file in onlyfiles:
                data = pd.read_csv(os.path.join(self.goodDataPath,file))
                for column in data.columns:
                    count = data[column][data[column]=='?'].count()
                    if count != 0:
                        data[column] = data[column].replace("?","'?'")
                data.to_csv(self.goodDataPath+"/"+file,index=None,header=True)
                self.logger.logger(logfile,file+" : Quotes added successfully")

        except Exception as e:
            self.logger.logger(logfile,"Data transfromation failed because :: "+str(e))

