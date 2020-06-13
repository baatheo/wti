import hashlib
from flask import Flask, request, make_response, jsonify
import hashlib
from joblib import load
import json
import pandas as pd


def getHash(item):
    item = str(item)
    tempItem = hashlib.sha224(item.encode("utf-8")).hexdigest()
    hashToReturn = int(tempItem, 16)
    return str(hashToReturn % 10000)


class apiLogic:
    def __init__(self):
        self.model = "/home/vagrant/PycharmProjects/wtiproj03/diabetes_clf"
        self.mlp = load(self.model + ".joblib")
        self.predictions = {}

    def getPrediction(self, value):
        hashedValue = getHash(value)
        if hashedValue in self.predictions:
            pred = self.predictions[hashedValue]
        else:
            pred = self.addPrediction(value)
            self.predictions[hashedValue] = pred
        return pred, hashedValue

    def addPrediction(self, value):
        newDf = {}
        for key in value:
            newDf[key] = [value[key]]
        df = pd.DataFrame(newDf)
        item = self.mlp.predict_proba(df)[0][0]
        return item

    def updateModel(self, filename):
        self.model = "/home/vagrant/PycharmProjects/wtiproj03/" + filename
        self.mlp = load(self.model + ".joblib")
        self.predictions = {}
        return "The model " + filename + " has been loaded successfully"


app = Flask(__name__)
restLogic = apiLogic()


@app.route('/patient-record', methods=["POST"])
def newRecord():
    sample = request.get_json()
    patRec, hashedID = restLogic.getPrediction(sample)
    return make_response(json.dumps({"patient_ID": hashedID}), 201)


@app.route('/patient-prediction/<patID>', methods=["GET"])
def patientPred(patID):
    patPred = restLogic.predictions[patID]
    return make_response(json.dumps({"probability-of-diabetes": patPred}), 200)


@app.route('/model', methods=["PUT"])
def newModel():
    model_data = request.get_json()
    file_name = model_data["new_model_file_name"]
    result = restLogic.updateModel(file_name)
    return make_response(jsonify(model_loading_result=result), 200)


if __name__ == '__main__':
    app.run(debug=True, port=9876)
