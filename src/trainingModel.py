from sklearn.model_selection import train_test_split
from src.data_ingestion import data_loader
from src.dataPreprocessing import preprocessing,clustering
from src.best_model_finder.Model_Finder import Model_Finder
from src.file_operations.File_operation import File_Operation
from src.application_logging.logger import App_logger

class train_Model:
    def __int__(self):
        self.logger = App_logger()
        self.file_object ="Training_Logs/ModelTrainingLog.txt"


    def modelTraining(self):
        self.logger.logger(self.file_object, 'Start of Training')
        try:
            data_getter =data_loader.dataLoader(self.file_object, self.logger)
            data = data_getter.get_data()

            preprocessor = preprocessing.Preprocessing(self.file_object, self.logger)
            data = preprocessor.replaceInvalidValuesWithNull(data)

            is_null_present,col_with_missing_values = preprocessor.is_null_present(data)

            if(is_null_present):
                data = preprocessor.impute_missing_values(data,col_with_missing_values)

            X,y =preprocessor.separate_label_feature(data,label_column_name='Result')

            kmeans = clustering.KMeansClustering(self.file_object, self.logger)
            number_of_clusters = kmeans.elbow_plot(X)

            X = kmeans.create_cluster(X,number_of_clusters)
            X['Labels']  = y

            list_of_clusters = X['Cluster'].unique()

            for i in list_of_clusters:
                cluster_data =X[X['Cluster']==i]
                cluster_features = cluster_data.drop(['Labels','Cluster'],axis=1)
                cluster_label = cluster_data['Labels']

                x_train,x_test,y_train,y_test = train_test_split(cluster_features,cluster_label,test_size=1/3,random_state=36)
                model_finder = Model_Finder(self.file_object, self.logger)

                best_model_name,best_model = model_finder.get_best_model(x_train,y_train,x_test,y_test)
                file_operation = File_Operation(self.file_object, self.logger)
                file_operation.save_model(best_model,best_model_name+str(i))
            self.logger.logger(self.file_object, 'Successful End of Training')
        except Exception as e:
            self.logger.logger(self.file_object, 'Unsuccessful End of Training')
            raise e