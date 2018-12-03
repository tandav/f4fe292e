# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql.types import *
import datetime
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error

# ======================================================================

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()



n_partitions = 15 # adjust it manually, it should be >= than n_shops * n_items



# ======================================================================

def random_time_series(t, amp, phase):
    return int(np.round(
        amp * np.abs(
            np.sin(t - phase) + np.cos(t**3) + np.random.random()
        )
    ))

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

date_range = pd.date_range(start='2015-03-01', periods=days, freq='D')
t_range = np.linspace(0, 50, days)

for timestamp, t in zip(date_range, t_range):
    for shop in shops:
        for item in items:
            amp   = sales_amp_phase.loc[shop, item]['amp']
            phase = sales_amp_phase.loc[shop, item]['phase']
            sale  = random_time_series(t, amp, phase)
            date = datetime.date(
                year  = timestamp.year,
                month = timestamp.month,
                day   = timestamp.day
            )
            # if sale > 0:
            #     sales_data.append([date, shop, item, sale])
            sales_data.append([date, shop, item, sale])


split_date_pandas = date_range[-56]
split_date = datetime.date(
    year  = split_date_pandas.year,
    month = split_date_pandas.month,
    day   = split_date_pandas.day
)

split_date2_pandas = date_range[-28]
split_date2 = datetime.date(
    year  = split_date2_pandas.year,
    month = split_date2_pandas.month,
    day   = split_date2_pandas.day
)
sales = spark.createDataFrame(data = sales_data, schema = schema)
sales.write.save('sales', format='parquet', mode='overwrite')
# sales.write.saveAsTable('sales', mode='overwrite')
