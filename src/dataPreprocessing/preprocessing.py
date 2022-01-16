import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer
from sklearn_pandas import CategoricalImputer

class Preprocessing:
    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object


    def remove_columns(self,data,columns):
        self.logger_object.logger(self.file_object,'Entered the remove_columns method of the Preprocessor class')
        self.data = data
        self.columns = columns
        try:
            self.useful_data = self.data.drop(labels=self.columns,axis=1)
            self.logger_object.logger(self.file_object,'Column removal Successful.Exited the remove_columns method of the Preprocessor class')
            return self.useful_data
        except Exception as e:
            self.logger_object.logger(self.file_object,'Exception occured in remove_columns method of the Preprocessor class. Exception message:  '+str(e))
            self.logger_object.logger(self.file_object,
                                      'Column removal Unsuccessful. Exited the remove_columns method of the Preprocessor class')
            raise e


    def separate_label_feature(self, data, label_column_name):
        self.logger_object.logger(self.file_object, 'Entered the separate_label_feature method of the Preprocessor class')
        try:
            self.X = data.drop(labels=label_column_name,axis=1)
            self.Y = data[label_column_name]
            self.logger_object.log(self.file_object,
                               'Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class')
            return self.X,self.Y
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in separate_label_feature method of the Preprocessor class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
            raise e

    def dropUnnecessaryColumns(self, data, columnNameList):
        data = data.drop(columnNameList,axis=1)
        return data

    def replaceInvalidValuesWithNull(self,data):
        for col in data:
            count = data[col][data[col]=='?'].count()
            if count !=0:
                data[col] = data[col].replace('?',np.nan)
        return data

    def is_null_present(self,data):
        self.logger_object.logger(self.file_object, 'Entered the is_null_present method of the Preprocessor class')
        self.null_present=False
        self.cols_with_missing_values=[]
        self.cols = data.columns
        try:
            self.null_counts = data.isna().sum()
            for i in range(len(self.null_counts)):
                if self.null_counts[i]>0:
                    self.null_present =True
                    self.cols_with_missing_values.append(self.cols[i])
            if self.null_present:
                self.dataframe_with_null=pd.DataFrame()
                self.dataframe_with_null['columns'] = data.columns
                self.dataframe_with_null['missing values count'] = np.asarray(data.isna().sum())
                self.dataframe_with_null.to_csv('preprocessing_data/null_values.csv') # storing the null column information to file
            self.logger_object.logger(self.file_object,'Finding missing values is a success.Data written to the null values file. Exited the is_null_present method of the Preprocessor class')
            return self.null_present,self.cols_with_missing_values
        except Exception as e:
            self.logger_object.logger(self.file_object,
                                  'Exception occured in is_null_present method of the Preprocessor class. Exception message:  ' + str(
                                      e))
            self.logger_object.logger(self.file_object,
                                   'Finding missing values failed. Exited the is_null_present method of the Preprocessor class')
            raise e

    def encodeCategoricalValues(self,data):
        data['class'] = data["class"].map({'p':1,'e':2})
        for column in data.drop(['class'],axis=1).columns:
            data = pd.get_dummies(data,columns=[column])
        return data

    def encodeCategoricalValuesPrediction(self,data):
        for column in data.columns:
            data = pd.get_dummies(data, columns=[column])

        return data


    def impute_missing_values(self, data, cols_with_missing_values):
        self.logger_object.logger(self.file_object, 'Entered the impute_missing_values method of the Preprocessor class')
        self.data = data
        self.cols_with_missing_values = cols_with_missing_values
        try:
            self.imputer = CategoricalImputer()
            for col in self.cols_with_missing_values:
                self.data[col] = self.imputer.fit_transform(self.data[col])
            self.logger_object.logger(self.file_object, 'Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class')
            return self.data
        except Exception as e:
            self.logger_object.logger(self.file_object,
                                   'Exception occured in impute_missing_values method of the Preprocessor class. Exception message:  ' + str(
                                       e))
            self.logger_object.logger(self.file_object,
                                   'Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class')

            raise e

    def get_columns_with_zero_std_deviation(self,data):
        self.logger_object.logger(self.file_object, 'Entered the get_columns_with_zero_std_deviation method of the Preprocessor class')
        self.columns = data.columns
        self.data_n =data.describe()
        self.col_to_drop =[]
        try:
            for x in self.columns:
                if (self.data_n[x]['std'] == 0): # check if standard deviation is zero
                    self.col_to_drop.append(x)  # prepare the list of columns with standard deviation zero
            self.logger_object.logger(self.file_object, 'Column search for Standard Deviation of Zero Successful. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            return self.col_to_drop
        except Exception as e:
            self.logger_object.logger(self.file_object,
                                   'Exception occured in get_columns_with_zero_std_deviation method of the Preprocessor class. Exception message:  ' + str(
                                       e))
            self.logger_object.logger(self.file_object,
                                   'Column search for Standard Deviation of Zero Failed. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            raise e