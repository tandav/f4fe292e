# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql.types import *
import datetime
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# ======================================================================
spark = SparkSession.builder.getOrCreate()
# ======================================================================

from random import random
from math import sin, cos

def random_time_series(date, amp, phase):
    base = datetime.date(year=2000, month=1, day=1)
    t = (date - base).days
    return int(round(amp * abs(sin(t - phase) + cos(t**3))) + random())

# ======================================================================

# shops = list(range(10)) # 10 shops in this dataset
# items = list(range(50)) # 50 items in this dataset

# very small for developing
shops = [0, 1, 2]
items = [0, 1, 2, 3]

# ======================================================================

sales_amp_phase = pd.DataFrame(
    columns = ['amp', 'phase'],
    index=pd.MultiIndex.from_product([shops, items], names=['shop', 'item'])
)

for shop in shops:
    for item in items:
        sales_amp_phase.loc[shop, item]['amp'] = np.random.exponential(scale=10)
        sales_amp_phase.loc[shop, item]['phase'] = np.random.random() * 40
sales_amp_phase

# ======================================================================

schema = StructType([
    StructField(name = 'date', dataType = DateType()   , nullable=False),
    StructField(name = 'shop', dataType = LongType()   , nullable=False),
    StructField(name = 'item', dataType = LongType()   , nullable=False),
    StructField(name = 'sale', dataType = IntegerType(), nullable=False),
])

sales_data = []
# days = 10
# days = 1056
days = 256

start = datetime.date(year=2015, month=3, day=1)
date_range = [start + datetime.timedelta(days=x) for x in range(days)]


for date in date_range:
    for shop in shops:
        for item in items:
            amp   = sales_amp_phase.loc[shop, item]['amp']
            phase = sales_amp_phase.loc[shop, item]['phase']
            sale  = random_time_series(date, amp, phase)
            # if sale > 0:
            #     sales_data.append([date, shop, item, sale])
            sales_data.append([date, shop, item, sale])


split_date  = date_range[-56]
split_date2 = date_range[-28]

sales = spark.createDataFrame(data = sales_data, schema = schema)
sales.write.save('sales', format='parquet', mode='overwrite')
# sales.write.saveAsTable('sales', mode='overwrite')

