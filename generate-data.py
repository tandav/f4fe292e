from pyspark.sql.types import *
import datetime
from pyspark.sql import SparkSession
import sys

# ==========================================    ============================
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
shops = [0, 1, 2]
items = [0, 1, 2, 3]

# ======================================================================

# fixed
sales_amp_phase = {
    (0, 0, 'amp'): 0.150, (0, 0, 'phase'): 7.8586,
    (0, 1, 'amp'): 0.076, (0, 1, 'phase'): 18.153,
    (0, 2, 'amp'): 0.006, (0, 2, 'phase'): 30.449,
    (0, 3, 'amp'): 0.076, (0, 3, 'phase'): 37.141,
    (1, 0, 'amp'): 0.016, (1, 0, 'phase'): 29.471,
    (1, 1, 'amp'): 0.082, (1, 1, 'phase'): 31.124,
    (1, 2, 'amp'): 0.053, (1, 2, 'phase'): 16.408,
    (1, 3, 'amp'): 0.153, (1, 3, 'phase'): 15.013,
    (2, 0, 'amp'): 0.148, (2, 0, 'phase'): 1.0655,
    (2, 1, 'amp'): 0.099, (2, 1, 'phase'): 7.2022,
    (2, 2, 'amp'): 0.063, (2, 2, 'phase'): 35.497,
    (2, 3, 'amp'): 0.048, (2, 3, 'phase'): 32.801
}

# or generate new one:
# sales_amp_phase = {}
# for shop in shops:
#     for item in items:
#         sales_amp_phase[(shop, item, 'amp')]   = expovariate(10)
#         sales_amp_phase[(shop, item, 'phase')] = random() * 40


# ======================================================================

schema = StructType([
    StructField(name = 'date', dataType = DateType()   , nullable=False),
    StructField(name = 'shop', dataType = LongType()   , nullable=False),
    StructField(name = 'item', dataType = LongType()   , nullable=False),
    StructField(name = 'sale', dataType = IntegerType(), nullable=False),
])

sales_data = []


start = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
# start = datetime.date(year=2015, month=3, day=1)
days = int(sys.argv[2])


date_range = [start + datetime.timedelta(days=x) for x in range(days)]


for date in date_range:
    for shop in shops:
        for item in items:
            amp   = sales_amp_phase[(shop, item, 'amp')]
            phase = sales_amp_phase[(shop, item, 'phase')]
            sale  = random_time_series(date, amp, phase)
            # if sale > 0:
            #     sales_data.append([date, shop, item, sale])
            sales_data.append([date, shop, item, sale])


split_date  = date_range[-56]
split_date2 = date_range[-28]

sales = spark.createDataFrame(data = sales_data, schema = schema)

table_name = sys.argv[3]
sales.write.save(table_name, format='parquet', mode='overwrite')
# sales.write.saveAsTable('sales', mode='overwrite')

