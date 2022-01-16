from wsgiref import simple_server
from flask import Flask,request,Response
import flask_monitoringdashboard as dashboard
import os
from flask_cors import CORS,cross_origin
from src.trainingModel import train_Model
from src.training_Validation_insertion import train_validation


#trainingModel.hello()

obj = train_Model()
obj.modelTraining()

"""os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
dashboard.bind(app)
CORS(app)

@app.route("/train",methods=["POST"])
@cross_origin()
def trainRouiteClient():
    try:
        if request.json['folderpath'] is not None:
            path = request.json['folderpath']
            train_valObj = train_validation(path)
            train_valObj.training_validaiton()

            trainModelObj = train_Model()
            trainModelObj.trainModel()

    except Exception as e:
        return Response("Error occurred! " + str(e))
    return Response("Training is successfully!!")


DEFAULT_PORT =1234
port = int(os.getenv("PORT",DEFAULT_PORT))
if __name__ == "__main__":
    host="0.0.0.0"
    app.run(debug=True)
    httpd = simple_server.make_server(host, port, app)
    print("Serving on %s %d" % (host, port))
    httpd.serve_forever()
"""

