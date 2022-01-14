from datetime import datetime

class App_logger:
    def __init__(self):
        pass

    def logger(self,file,log_message):
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time =self.now.strftime("%H:%M:%S")
        self.file_object =open(file,"a+")
        self.file_object.write(str(self.date)+"/"+str(self.current_time)+"\t\t"+log_message)
        self.file_object.close()
