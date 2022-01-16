from sklearn.svm import SVC
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score,accuracy_score

class Model_Finder:
    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.sv_classifier = SVC()
        self.xgb = XGBClassifier(objective='binary:logistic',n_jobs=-1)


    def get_best_params_for_svm(self,train_x,train_y):
        self.logger_object.logger(self.file_object, 'Entered the get_best_params_for_svm method of the Model_Finder class')
        try:
            self.param_grid = {"kernel": ['rbf', 'sigmoid'],
                               "C": [0.1, 0.5, 1.0],
                               "random_state": [0, 100, 200, 300]}
            self.grid = GridSearchCV(estimator=self.sv_classifier,param_grid=self.param_grid,cv=5,verbose=3)
            self.grid.fit(train_x,train_y)

            self.kernel = self.grid.best_params_['kernel']
            self.C = self.grid.best_params_['C']
            self.random_state = self.grid.best_params_['random_state']

            self.sv_classifier= SVC(kernel=self.kernel,C=self.C,random_state=self.random_state)
            self.sv_classifier.fit(train_x,train_y)
            self.logger_object.logger(self.file_object,
                               'SVM best params: ' + str(
                                   self.grid.best_params_) + '. Exited the get_best_params_for_svm method of the Model_Finder class')
            return self.sv_classifier
        except Exception as e:
            self.logger_object.logger(self.file_object,
                                   'Exception occured in get_best_params_for_svm method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.logger(self.file_object,
                                   'SVM training  failed. Exited the get_best_params_for_svm method of the Model_Finder class')
            raise e

    def get_best_params_for_xgboost(self, train_x, train_y):
        self.logger_object.logger(self.file_object,
                               'Entered the get_best_params_for_xgboost method of the Model_Finder class')

        try:
            self.param_grid_xgboost = {

                "n_estimators": [100, 130], "criterion": ['gini', 'entropy'],
                "max_depth": range(8, 10, 1)

            }

            self.grid = GridSearchCV(XGBClassifier(objective='binary:logistic'),self.param_grid_xgboost,verbose=3,cv=5)
            self.grid.fit(train_x,train_y)

            self.criterion = self.grid.best_params_['criterion']
            self.max_depth = self.grid.best_params_['max_depth']
            self.n_estimators = self.grid.best_params_['n_estimators']

            self.xgb = XGBClassifier(criterion=self.criterion,max_depth=self.max_depth,n_estimators=self.n_estimators,n_jobs=-1)
            self.xgb.fit(train_x,train_y)
            self.logger_object.logger(self.file_object,
                                   'XGBoost best params: ' + str(
                                       self.grid.best_params_) + '. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            return self.xgb
        except Exception as e:
            self.logger_object.logger(self.file_object,
                                   'Exception occured in get_best_params_for_xgboost method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.logger(self.file_object,
                                   'XGBoost Parameter tuning  failed. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            raise e

    def get_best_model(self,train_x,train_y,test_x,test_y):
        self.logger_object.logger(self.file_object,
                               'Entered the get_best_model method of the Model_Finder class')
        try:
            self.xgboost = self.get_best_params_for_xgboost(train_x,train_y)
            self.prediction_xgboost = self.xgboost.predict(test_x)

            if len(test_y.unique()) ==1:
                self.xgboost_score = accuracy_score(test_y,self.prediction_xgboost)
                self.logger_object.logger(self.file_object, 'Accuracy for XGBoost:' + str(self.xgboost_score))  # Log AUC
            else:
                self.xgboost_score = roc_auc_score(test_y,self.prediction_xgboost)
                self.logger_object.logger(self.file_object, 'AUC for XGBoost:' + str(self.xgboost_score)) # Log AUC

            self.svm = self.get_best_params_for_svm(train_x,train_y)
            self.prediction_svm = self.svm.predict(test_x)

            if len(test_y.unique())==1:
                self.svm_score = accuracy_score(test_y,self.prediction_svm)
                self.logger_object.logger(self.file_object, 'Accuracy for SVM:' + str(self.svm_score))
            else:
                self.svm_score = roc_auc_score(test_y,self.prediction_svm)
                self.logger_object.logger(self.file_object, 'Accuracy for SVM:' + str(self.svm_score))

            if(self.svm_score < self.xgboost_score):
                return 'XGBoost',self.xgboost
            else:
                return 'SVM',self.sv_classifier

        except Exception as e:
            self.logger_object.logger(self.file_object,
                                   'Exception occured in get_best_model method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.logger(self.file_object,
                                   'Model Selection Failed. Exited the get_best_model method of the Model_Finder class')
            raise e





