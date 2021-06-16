#---------------------------IMPORTACION DE LIBRERIAS------------------------------------
from apache_beam.transforms.trigger import AfterProcessingTime
from apache_beam.transforms.window import FixedWindows
from flask import Flask, request, jsonify
import json
import apache_beam as beam
from apache_beam.transforms.userstate import BagStateSpec, CombiningValueStateSpec
from apache_beam import coders
from apache_beam.options.pipeline_options import PipelineOptions

class Server():

  def __init__(self,buffer):
    self.app = Flask(__name__)
    self.buffer = buffer

#-------------------------FIN IMPORTACION DE LIBRERIAS----------------------

#---------------------------INICIALIZACION DE VARIABLES GLOBALES----------
#-------------------------FIN INICIALIZACION DE VARIABLES GLOBALES-----------------------------------
  # @self.app.route("/", methods=["POST", "GET"])

  def hello(self):
    print("request:",request.json)
    temperatura = request.json["data"][0]["temperature"]["value"]
    # timestamp = request.json["data"][0]["timestamp"]["value"]
    # print("valor de la temperatura:", temperatura)
    self.buffer.append(temperatura)
    # for x in buffer:
    #  print(x)
    # print(buffer)
    # print(self.buffer)
    return jsonify(self.buffer)
    #return "", str(temperatura)


  def run(self, port):
    self.app.add_url_rule("/", view_func=self.hello, methods=["POST", "GET"])
    self.app.run(host='localhost', port=port, debug=False)
    print("FUNCIONA MAS")
    return jsonify(self.buffer)
