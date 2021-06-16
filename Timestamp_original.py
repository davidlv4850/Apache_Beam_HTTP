import apache_beam as beam
from apache_beam.transforms.window import (
    TimestampedValue,
    Sessions,
    Duration,
)
from apache_beam.io.textio import WriteToText

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


# User defined functions should always be subclassed from DoFn. This function transforms
# each element into a tuple where the first field is userId and the second is click. It
# assigns the timestamp to the metadata of the element such that window functions can use
# it later to group elements into windows.
class AddTimestampDoFn(beam.DoFn):
    def process(self, element):
        unix_timestamp = element["timestamp"]
        element = (element["userId"], element["temperature"])

        yield TimestampedValue(element, unix_timestamp)


with beam.Pipeline() as p:
    # fmt: off
    events = p | beam.Create(
        [
            {"userId": "Andy", "temperature": 20, "timestamp": 1603112520},  # Event time: 13:02
            {"userId": "Sam", "temperature": 39, "timestamp": 1603113240},  # Event time: 13:14
            {"userId": "Andy", "temperature": 12, "timestamp": 1603115820},  # Event time: 13:57
            {"userId": "Andy", "temperature": 16, "timestamp": 1603113600},  # Event time: 13:20
        ]
    )
    # fmt: on

    # Assign timestamp to metadata of elements such that Beam's window functions can
    # access and use them to group events.
    timestamped_events = events | "AddTimestamp" >> beam.ParDo(AddTimestampDoFn())

    windowed_events = timestamped_events | beam.WindowInto(
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

    # We can use CombinePerKey with the predifined sum function to combine all elements
    # for each key in a collection.
    sum_clicks = windowed_events | beam.CombinePerKey(AverageFn()) | beam.Map(print)

    # WriteToText writes a simple text file with the results.
    # sum_clicks | WriteToText(file_path_prefix="output")