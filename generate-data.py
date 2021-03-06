from pyspark.sql.types import *
import datetime
from pyspark.sql import SparkSession
import sys
import argparse
import pickle

parser = argparse.ArgumentParser()
parser.add_argument('start'               , type=str                           )
parser.add_argument('days'                , type=int                           )
parser.add_argument('--shops'             , type=int           , required=True )
parser.add_argument('--items'             , type=int           , required=True )
parser.add_argument('--use_last_generator', action='store_true', required=False)
parser.add_argument('table'               , type=str                           )
args = parser.parse_args()


# ======================================================================
spark = SparkSession.builder.getOrCreate()
# ======================================================================

from random import random, expovariate
from math import sin, cos

def random_time_series(date, amp, phase):
    base = datetime.date(year=2000, month=1, day=1)
    t = (date - base).days
    return int(round(amp * abs(sin(t - phase) + cos(t**3))) + random())

# ======================================================================

# very small for developing
shops = list(range(args.shops))
items = list(range(args.items))

# ======================================================================



if args.use_last_generator:
    with open('generator.pickle', 'rb') as handle:
        sales_amp_phase = pickle.load(handle)
else:
    # fixed amp and phase
    # sales_amp_phase =  {
    #     (0, 0, 'amp'): 3.778, (0, 0, 'phase'): 36.00,
    #     (0, 1, 'amp'): 17.76, (0, 1, 'phase'): 20.07,
    #     (0, 2, 'amp'): 17.36, (0, 2, 'phase'): 2.840,
    #     (0, 3, 'amp'): 10.25, (0, 3, 'phase'): 35.65,
    #     (1, 0, 'amp'): 15.36, (1, 0, 'phase'): 1.160,
    #     (1, 1, 'amp'): 0.189, (1, 1, 'phase'): 35.79,
    #     (1, 2, 'amp'): 17.35, (1, 2, 'phase'): 32.29,
    #     (1, 3, 'amp'): 1.904, (1, 3, 'phase'): 34.64,
    #     (2, 0, 'amp'): 2.008, (2, 0, 'phase'): 33.55,
    #     (2, 1, 'amp'): 4.186, (2, 1, 'phase'): 19.75,
    #     (2, 2, 'amp'): 19.36, (2, 2, 'phase'): 7.907,
    #     (2, 3, 'amp'): 17.10, (2, 3, 'phase'): 33.62,
    # }

    # or generate new randomly:
    sales_amp_phase = {}
    for shop in shops:
        for item in items:
            sales_amp_phase[(shop, item, 'amp')]   = expovariate(0.1)
            sales_amp_phase[(shop, item, 'phase')] = random() * 40

with open('generator.pickle', 'wb') as handle:
    pickle.dump(sales_amp_phase, handle, protocol=pickle.HIGHEST_PROTOCOL)

# ======================================================================

schema = StructType([
    StructField(name = 'date', dataType = DateType()   , nullable=False),
    StructField(name = 'shop', dataType = LongType()   , nullable=False),
    StructField(name = 'item', dataType = LongType()   , nullable=False),
    StructField(name = 'sale', dataType = IntegerType(), nullable=False),
])

start = datetime.datetime.strptime(args.start, '%Y-%m-%d').date()
days = args.days
date_range = [start + datetime.timedelta(days=x) for x in range(days)]

sales_data = []


for date in date_range:
    for shop in shops:
        for item in items:
            amp   = sales_amp_phase[(shop, item, 'amp')]
            phase = sales_amp_phase[(shop, item, 'phase')]
            sale  = random_time_series(date, amp, phase)
            if sale > 0:
                sales_data.append([date, shop, item, sale])
            # sales_data.append([date, shop, item, sale])

sales = spark.createDataFrame(data = sales_data, schema = schema)
table_name = args.table
sales.write.save(table_name, format='parquet', mode='overwrite')
# sales.write.saveAsTable('sales', mode='overwrite')

