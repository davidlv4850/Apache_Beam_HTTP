#---------------------------IMPORTACION DE LIBRERIAS------------------------------------
from apache_beam.transforms.trigger import AfterProcessingTime
from apache_beam.transforms.window import FixedWindows
from flask import Flask, request, jsonify
import globales
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

app = Flask(__name__)
#-------------------------FIN IMPORTACION DE LIBRERIAS----------------------

#---------------------------INICIALIZACION DE VARIABLES GLOBALES----------
#-------------------------FIN INICIALIZACION DE VARIABLES GLOBALES-----------------------------------
buffer =[]
@app.route("/", methods=["POST", "GET"])
def hello():
    print("request:",request.json)
    temperatura = request.json["data"][0]["temperature"]["value"]
    print("valor de la temperatura:", temperatura)
    buffer.append(temperatura)
    for x in buffer:
     print(x)
    print(buffer)
    return jsonify(buffer)

def run(port):
    print("FUNCIONA")
    app.run(host='localhost', port=port, debug=True)
    print("FUNCIONA MAS")


# if __name__ == "__main__":
#     run(9001)
