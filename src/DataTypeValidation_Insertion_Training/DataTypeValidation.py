import shutil
import sqlite3
import os
from datetime import datetime
from os import listdir
import csv
from src.application_logging.logger import App_logger
import src.Generic.Utility as utility

class dbOperation:
    def __init__(self):
        config = utility.read_yamlfile(os.path.join("config","params.yaml"))
        self.path = config['db']['training']
        self.badFilePath = config['training_filepath']['bad_raw']
        self.goodFilePath = config['training_filepath']['good_raw']
        self.logger = App_logger()

    def dbConnection(self,DatabaseName):
        logFile = "Logs/Training_Logs/DataBaseConnectionLog.txt"
        try:
            conn = sqlite3.connect(DatabaseName+".db")
            self.logger.logger(logFile,"Opened " +DatabaseName+ " connection successfully")
        except Exception as ex:
            self.logger.logger(logFile,"Error while connecting to database "+str(ex))
            raise ex
        return conn


    def createTableDB(self,DatabaseName,column_names):
        log_file= "Logs/Training_Logs/DbTableCreateLog.txt"
        try:
            conn =self.dbConnection(DatabaseName+".db")
            cursor = conn.cursor()
            cursor.execute("SELECT count(name) FROM sqlite_master WHERE type = 'table' AND name = 'Good_Raw_Data'")
            if cursor.fetchone()[0] ==1:
                self.logger.logger(log_file,"Good_Raw_Data Table os created successfully")
            else:
                for key in column_names.keys():
                    type = column_names[key]
                    try:
                        conn.execute("ALTER Table Good_Raw_Data Add COLUMN '{column_name}' {datatype}".format(column_name=key,datatype=type))
                    except:
                        conn.execute("CREATE Table Good_Raw_Data ({column_name} {datatype})".format(column_name=key,datatype=type))
                conn.close()
                self.logger.logger(log_file,"Tables created successfully")
                self.logger.logger("Logs/Training_Logs/DataBaseConnectionLog.txt","Closed %s database successfully" % DatabaseName)
        except Exception as e:
            self.logger.logger(log_file,"Error while creating table "+ str(e))
            conn.close()
            self.logger.logger("Logs/Training_Logs/DataBaseConnectionLog.txt","Closed "+DatabaseName+ " database successfully.")
            raise e


    def insertIntoTableGoodData(self,Database):
        log = "Logs/Training_Logs/DbInsertLog.txt"
        conn =self.dbConnection(Database)
        goodFilepath = self.goodFilePath
        badFilepath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilepath)]
        for file in onlyfiles:
            try:
                with open(goodFilepath+"/"+file,"r") as f:
                    next(f)
                    reader =csv.reader(f,delimiter="\n")
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute("Insert into Good_Raw_Data values ({values})".format(values=(list_)))
                                self.logger.logger(log,"%s: File loaded successfully!! " %file)
                                conn.commit()
                            except Exception as ex:
                                raise ex
            except Exception as e:
                conn.rollback()
                self.logger.logger(log,"Error while creating table: %s " %e)
                shutil.move(goodFilepath+"/"+file,badFilepath)
                self.logger.logger(log,"File moved successfully %s "%file)
                conn.close()
        conn.close()

    def selectingDatafromtableintocsv(self,database):
        self.fileFromDb = 'Training_FilefromDB/'
        self.fileName = 'InputFile.csv'
        log = "Logs/Training_Logs/ExportToCsv.txt"
        try:
            conn = self.dbConnection(database)
            query = "select * from Good_Raw_Data"
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            headers = [i[0] for i in cursor.description]
            utility.createDirectory(self.fileFromDb)
            csvFile = csv.writer(open(self.fileFromDb+self.fileName,'w',newline=''),delimiter=',',lineterminator="\r\n",quoting=csv.QUOTE_ALL,escapechar="\\")
            csvFile.writerow(headers)
            csvFile.writerows(results)
            self.logger.logger(log,"File exported successfully!!")
        except Exception as e:
            self.logger.logger(log,"File exporting failed. Error : %s" %e)


