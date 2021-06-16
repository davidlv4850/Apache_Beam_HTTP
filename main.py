import apache_beam as beam
from apache_beam import DoFn, Pipeline
from apache_beam.coders import VarIntCoder
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners import direct
from apache_beam.transforms import trigger
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode, Repeatedly, AfterWatermark, AfterCount
from apache_beam.utils.windowed_value import WindowedValue
from apache_beam.transforms.window import FixedWindows, SlidingWindows, GlobalWindow, Sessions, TimestampedValue
from apache_beam.transforms.userstate import BagStateSpec, CombiningValueStateSpec, TimerSpec, on_timer
from apache_beam import coders, combiners, CombineGlobally, TimeDomain
import threading, time, json
from time import sleep,localtime
from apache_beam.utils.timestamp import Timestamp

from flask import Flask

app = Flask(__name__)

from Servidor import Server


class NGSI():
    id: str
    type: str
    co: float
    co2: float
    pressure: float
    temperature: float
    wind_speed: float
    timestamp: float

beam.coders.registry.register_coder(NGSI, beam.coders.RowCoder)

class CounterFn(beam.CombineFn):
    def __init__(self):
        self.counter = 0
    def count(self):
        self.counter = self.counter + 1
        return self.counter

class AddTimestampDoFnTemp(beam.DoFn):
    def process(self, element):
        unix_timestamp = element.localtime.now()
        element = (element["id"], element["temperature"])
        yield TimestampedValue(element, unix_timestamp)

class DoFnMethods(DoFn):
    def __init__(self):
        self.buffer = [9]
        print('__init__')
        self.window = SlidingWindows(20, 5)

    def setup(self):
        server = Server(self.buffer)
        hilo_2 = threading.Thread(target=server.run, name='Run', args=[9001])
        hilo_2.start()
        print('setup')

    def start_bundle(self):
        print('start_bundle', time.localtime())

    def process(self, element, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam,
                # state_counter=beam.DoFn.StateParam(CombiningValueStateSpec('counter', VarIntCoder(), CounterFn())),
                state_buffer=beam.DoFn.StateParam(BagStateSpec('buffer', VarIntCoder())),
                # max_timestamp=beam.DoFn.StateParam(CombiningValueStateSpec('max_timestamp_seen', max)),
                # timer=DoFn.TimerParam(TimerSpec('timer', TimeDomain.REAL_TIME))
                ):

        for x in self.buffer:
          print(x)
          state_buffer.add(x)
          unix_timestamp = timestamp
          print(state_buffer)
          element = x
          yield TimestampedValue(element, unix_timestamp)
        print("process", time.localtime())
        lock = threading.Lock()
        lock.acquire()
        # MAX_BUFFER_SIZE = 10
        # timer.set(Timestamp(), sleep(30))
        # state_counter.add(1)
        # count = state_counter.read()
        count = len(self.buffer)
        buffer = state_buffer.read()
        # if count >= MAX_BUFFER_SIZE:
        #  for event in buffer:
        #     yield event
        #  for x in self.buffer:
        #     self.buffer.pop(x)
        # state_buffer.clear()
        lock.release()
''''
        @on_timer(TimerSpec(timer))
        def timer_callback(self, state_buffer=DoFn.StateParam(BagStateSpec('buffer', VarIntCoder())), timer_tag=DoFn.DynamicTimerTagParam):
         # Process timer.
         # state_counter.clear()
         
         state_buffer.clear()
         # max_timestamp.clear()
         yield (timer_tag, 'fired')
'''
def finish_bundle(self):
    yield WindowedValue(
            value='finish_bundle',
            timestamp=0,
            windows=[self.window]
    )

    def teardown(self):
        print('teardown')


# Método para hacer la media de los datos que me llegan
# con un valor máximo
def bounded_sum(values, bound=500):
    def create_accumulator(self):
        return (0.0, 0)

    def add_input(self, sum_count, input):
        (sum, count) = sum_count
        return sum + input, count + 1

    def merge_accumulators(self, accumulators):
        sums, counts = zip(*accumulators)
        return sum(sums), sum(counts)

    def extract_output(self, sum_count):
        (sum, count) = sum_count
        return sum / count if count else float('NaN')


'''class MyOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    streaming = True
    direct_running_mode = 'multi_processing'
    runner=direct
    parser.add_argument(
        runner=direct,
        help='Input for the pipeline',
        default='C://Users/david/TFG')
'''

if __name__ == '__main__':
    with Pipeline(options=PipelineOptions()) as p:
        results = (p
                   | 'Create inputs' >> beam.Create([0])
                   | "Add Dummy Key" >> beam.Map(lambda elem: (None, elem))
                   | 'DoFn methods' >> beam.ParDo(DoFnMethods())
                   | 'SlidingWindows' >> beam.WindowInto(Sessions(gap_size=10),
                                                         trigger=Repeatedly(AfterCount(1)),
                                                         accumulation_mode=AccumulationMode.DISCARDING)
                   | "TempPrint" >> beam.Map(print))

        average = (results
                   | 'SlidingWindows_average' >> beam.WindowInto(Sessions(10))
                   | 'average' >> beam.CombineGlobally(bounded_sum).without_defaults()
                   | 'output' >> beam.Map(print)
                   )
