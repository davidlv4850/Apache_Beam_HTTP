
import apache_beam as beam
from apache_beam import DoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.trigger import AfterProcessingTime
from apache_beam.transforms.window import FixedWindows
import threading
import time


from flask import Flask, request, jsonify
app = Flask(__name__)

from Servidor import run


def setup(buffer=[]):
    print(buffer,time.localtime())
    print('setup', time.localtime())


def process(self,port, buffer, element, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
    run(port)
    print(buffer,time.localtime())
    # for x in self.buffer:
    #     x = element
    #     all_values_in_buffer_1 = [x for x in self.buffer.values]
    #     yield element
    yield '\n'.join([
        '# timestamp'])
    # # Do you processing here
    # for x in buffer:
    #     x = element
    # # # Read all the data from buffer1
    # # all_values_in_buffer_1 = [x for x in buffer_1.read()]
    #
    # yield element




class DoFnMethods(beam.DoFn):
    def __init__(self):
        self.buffer = []
        print('__init__')
        self.window = beam.window.GlobalWindow()

    def setup(self):
        buffer = self.buffer
        print(buffer)
        # thread = Thread(target=run, args=(9001,))
        # thread.start()
        print('setup')

    def start_bundle(self):
       print('start_bundle',time.localtime())


    def process(self, element):
        hilo_1 = threading.Thread(target=setup, name='Buffer',args=(self.buffer))
        # hilo_2 = threading.Thread(target=process, name='Recogida', args=(self,element,self.buffer))
        hilo_2 = threading.Thread(target=run, name='Run',args=[9001])
        hilo_1.start()
        hilo_2.start()


    def finish_bundle(self):
        yield beam.utils.windowed_value.WindowedValue(
            value='finish_bundle',
            timestamp=0,
            windows=[self.window],
        )

    def teardown(self):
     print('teardown')


if __name__ == '__main__':
    with beam.Pipeline(options=PipelineOptions()) as p:
        results = (p
                   | 'Create inputs' >> beam.Create([0])
                   # | 'Fixed 11sec windows' >> beam.WindowInto(FixedWindows(11), trigger=AfterProcessingTime(60),
                   #                                            accumulation_mode=(11),
                   #                                            allowed_lateness=1)
                   | 'DoFn methods' >> beam.ParDo(DoFnMethods())
                   | beam.Map(print))
