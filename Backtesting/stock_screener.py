from zipline import run_algorithm
from zipline.api import attach_pipeline, pipeline_output
from zipline.pipeline import pipeline, CustomFactor, Pipeline
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import MovingAverageConvergenceDivergenceSignal, EWMA, SimpleMovingAverage
import pandas as pd
import numpy as np
np.seterr(divide='ignore', invalid='ignore')

def initialize(context):
    pipe = make_pipeline()
    attach_pipeline(pipe, 'make_pipeline')


def make_pipeline():
    ewma = EWMA.from_span(inputs=[USEquityPricing.close], window_length=20, span=15, )
    macd = MovingAverageConvergenceDivergenceSignal(inputs=[USEquityPricing.close])
    close = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=20)
    pipe = Pipeline(
        columns={
            'close': close,
            'macd': macd,
            'ewma': ewma
        }
    )
    return pipe


def before_trading_start(context, data):
    context.output = pipeline_output('make_pipeline')
    print(context.output.head(5))


start = pd.Timestamp('2017-1-1', tz='utc')
end = pd.Timestamp('2017-12-31', tz='utc')

# Fire off the backtest
result = run_algorithm(
    start=start,
    end=end,
    initialize=initialize,
    capital_base=10000,
    data_frequency='daily',
    bundle='quantopian-quandl'
)
