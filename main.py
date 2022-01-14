from wsgiref import simple_server
from flask import Flask,request,Response
import flask_monitoringdashboard as dashboard
import os
from flask_cors import CORS,cross_origin

from src import trainingModel

#trainingModel.hello()

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
dashboard.bind(app)
CORS(app)

@app.route("/train",methods=["POST"])
@cross_origin()
def trainRouiteClient():
    try:
       pass
    except ValueError:
        pass


import src.Generic.Utility as u
print(u.read_yamlfile(os.path.join("config", "params.yaml"))['config']['training_schema'])



'''
port = int(os.getenv("PORT"))
if __name__ == "__main__":
    host="0.0.0.0"
    app.debug(True)
    httpd = simple_server.make_server(host, port, app)
    print("Serving on %s %d" % (host, port))
    httpd.serve_forever()
'''

