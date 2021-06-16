import apache_beam as beam
from apache_beam import DoFn, Pipeline
from apache_beam.coders import VarIntCoder
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners import direct
from apache_beam.transforms import trigger
from apache_beam.transforms.sql import SqlTransform
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode, Repeatedly, AfterWatermark, AfterCount
from apache_beam.utils.windowed_value import WindowedValue
from apache_beam.transforms.window import FixedWindows, SlidingWindows, GlobalWindow, Sessions
from apache_beam.transforms.userstate import BagStateSpec, CombiningValueStateSpec, TimerSpec, on_timer
from apache_beam import coders, combiners, CombineGlobally, TimeDomain
import threading, time, json, typing
from time import sleep,localtime
from apache_beam.utils.timestamp import Timestamp

if __name__ == '__main__':
    class FruitRecipe (typing.NamedTuple):
        recipe: str
        fruit: str
        quantity: int
        unit_cost: float


    coders.registry.register_coder(FruitRecipe, coders.RowCoder)



    with Pipeline() as p:
     pc = (p | beam.Create([FruitRecipe("pie", "strawberry", 3, 1.5),
                           ...,
                           FruitRecipe("muffin", "blueberry", 2, 2.), ]))
     funciona = (pc | beam.Map(lambda x: beam.Row()).with_output_types(FruitRecipe))

#     Sql = (pc | SqlTransform("SELECT * FROM PCOLLECTION WHERE quantity > 1"))
