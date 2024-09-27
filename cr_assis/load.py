import sys, os, datetime, glob, time, math, configparser, json, yaml, requests
from bokeh.io import output_notebook
from bokeh.plotting import figure, show
from bokeh.models import LinearAxis, Range1d
# output_notebook()
import pandas as pd
import numpy as np
with open(f"{os.environ['HOME']}/.cryptobridge/private_key.yml", "rb") as f:
    data = yaml.load(f, Loader= yaml.SafeLoader)
for info in data:
    if "mongo" in info.keys():
        os.environ["MONGO_URI"] = info['mongo']
        os.environ["INFLUX_URI"] = info['influx']
        os.environ["INFLUX_MARKET_URI"] = info['influx_market']