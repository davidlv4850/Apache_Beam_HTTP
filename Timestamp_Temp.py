import apache_beam as beam
import time
from apache_beam.transforms.window import (
    TimestampedValue,
    Sessions,
    Duration, SlidingWindows,
)

from apache_beam.io.textio import WriteToText

# User defined functions should always be subclassed from DoFn. This function transforms
# each element into a tuple where the first field is userId and the second is click. It
# assigns the timestamp to the metadata of the element such that window functions can use
# it later to group elements into windows.

# Método para sumar hasta un determinado numero
def saturated_sum(values):
  max_value = 70
  return min(sum(values), max_value)

def format_temp_average(id, temp):
  if id:
    yield '{}{}'.format("El nodo " + id + " ha enviado una temperatura media de: ",str(temp) + " grados")

def format_press_average(id, press):
  if id:
    yield '{}{}'.format("El nodo " + id + " ha enviado una presion media de: ",str(press) + " pascales")

# Método para calcular la media de los datos entrantes
class AverageFn(beam.CombineFn):
  def create_accumulator(self):
    sum = 0.0
    count = 0
    accumulator = sum, count
    return accumulator

  def add_input(self, accumulator, input):
    sum, count = accumulator
    return sum + input, count + 1

  def merge_accumulators(self, accumulators):
    # accumulators = [(sum1, count1), (sum2, count2), (sum3, count3), ...]
    sums, counts = zip(*accumulators)
    # sums = [sum1, sum2, sum3, ...]
    # counts = [count1, count2, count3, ...]
    return sum(sums), sum(counts)

  def extract_output(self, accumulator):
    sum, count = accumulator
    if count == 0:
      return float('NaN')
    return sum / count

class AddTimestampDoFnTemp(beam.DoFn):
    def process(self, element):
        unix_timestamp = element["timestamp"]
        element = (element["id"], element["temperature"])


        yield TimestampedValue(element, unix_timestamp)


class AddTimestampDoFnPress(beam.DoFn):
    def process(self, element):
        unix_timestamp = element["timestamp"]
        element = (element["id"], element["pressure"])
        yield TimestampedValue(element, unix_timestamp)

with beam.Pipeline() as p:
    # fmt: off
    events = p | beam.Create(
        [
         {"id": "R1", "type": "Node", "co": 0, "co2": 0, "humidity": 40, "pressure": 54, "temperature": 32, "wind_speed": 1.06, "timestamp": 1603112520}, # Event time: 13:02
         {"id": "R1", "type": "Node", "co": 0, "co2": 0, "humidity": 40, "pressure": 21, "temperature": 18, "wind_speed": 1.06, "timestamp": 1603113240}, # Event time: 13:14
         {"id": "R1", "type": "Node", "co": 0, "co2": 0, "humidity": 40, "pressure": 90, "temperature": 9, "wind_speed": 1.06, "timestamp": 1603115820}, # Event time: 13:57
         {"id": "R1", "type": "Node", "co": 0, "co2": 0, "humidity": 40, "pressure": 32, "temperature": 76, "wind_speed": 1.06, "timestamp": 1603113600}, # Event time: 13:20
        ]
    )
    # fmt: on

    # Assign timestamp to metadata of elements such that Beam's window functions can
    # access and use them to group events.
    timestamped_events_temp = events | "AddTimestampTemp" >> beam.ParDo(AddTimestampDoFnTemp())

    timestamped_events_press = events | "AddTimestampPress" >> beam.ParDo(AddTimestampDoFnPress())


    windowed_events_temp = timestamped_events_temp | "WindowTemp" >> beam.WindowInto(
        # Each session must be separated by a time gap of at least 30 minutes (1800 sec)
        # Sessions(gap_size=10 * 60),
        SlidingWindows(20, 5),
        # Triggers determine when to emit the aggregated results of each window. Default
        # trigger outputs the aggregated result when it estimates all data has arrived,
        # and discards all subsequent data for that window.
        trigger=None,
        # Since a trigger can fire multiple times, the accumulation mode determines
        # whether the system accumulates the window panes as the trigger fires, or
        # discards them.
        accumulation_mode=None,
        # Policies for combining timestamps that occur within a window. Only relevant if
        # a grouping operation is applied to windows.
        timestamp_combiner=None,
        # By setting allowed_lateness we can handle late data. If allowed lateness is
        # set, the default trigger will emit new results immediately whenever late
        # data arrives.
        allowed_lateness=Duration(seconds=1 * 24 * 60 * 60),  # 1 day
    )
    # WriteToText writes a simple text file with the results.
    # windowed_events_temp | "WriteWindowTemp" >> WriteToText(file_path_prefix="output_window_temp")

    windowed_events_press = timestamped_events_press | "WindowPress" >> beam.WindowInto(
        # Each session must be separated by a time gap of at least 30 minutes (1800 sec)
        Sessions(gap_size=10 * 60),
        # Triggers determine when to emit the aggregated results of each window. Default
        # trigger outputs the aggregated result when it estimates all data has arrived,
        # and discards all subsequent data for that window.
        trigger=None,
        # Since a trigger can fire multiple times, the accumulation mode determines
        # whether the system accumulates the window panes as the trigger fires, or
        # discards them.
        accumulation_mode=None,
        # Policies for combining timestamps that occur within a window. Only relevant if
        # a grouping operation is applied to windows.
        timestamp_combiner=None,
        # By setting allowed_lateness we can handle late data. If allowed lateness is
        # set, the default trigger will emit new results immediately whenever late
        # data arrives.
        allowed_lateness=Duration(seconds=1 * 24 * 60 * 60),  # 1 day
    )

    # WriteToText writes a simple text file with the results.
    # windowed_events_press | "WriteWindowPress" >> WriteToText(file_path_prefix="output_window_press")

    # We can use CombinePerKey with the predifined AverageFn function to combine all elements
    # for each key in a collection.
    average_temp = windowed_events_temp | "AverageTemp" >> beam.CombinePerKey(AverageFn()) | 'FormatTemp' >> beam.FlatMapTuple(format_temp_average) | "TempPrint" >> beam.Map(print)
    average_press = windowed_events_press | "AveragePress" >> beam.CombinePerKey(AverageFn()) | 'FormatPress' >> beam.FlatMapTuple(format_press_average) | "PressPrint" >> beam.Map(print)


    # WriteToText writes a simple text file with the results.
    # sum_clicks_temp | "WriteAverageTemp" >> WriteToText(file_path_prefix="output_temp")

    # WriteToText writes a simple text file with the results.
    # sum_clicks_press | "WriteAveragePress" >> WriteToText(file_path_prefix="output_press")