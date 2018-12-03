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


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()



n_partitions = 15 # adjust it manually, it should be >= than n_shops * n_items


# sales = spark.sql('select * from sales')
sales = spark.read.parquet('sales')
features = spark.read.parquet('features')

import pickle
with open('models.pickle', 'rb') as handle:
    models = pickle.load(handle)

import sys




if len(sys.argv) < 3:
    print('pass shop and item:')
    print('spark-submit predict.py 1 2')
    print('(shop=1 and item=2)')
    sys.exit(0)

shop = int(sys.argv[1])
item = int(sys.argv[2])

key = (shop, item)
# key = (0, 2) # (shop, item)

print(key)

# days = 10
# days = 1056
days = 256

date_range = pd.date_range(start='2015-03-01', periods=days, freq='D')
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

# ======================================================================

features_test = features.filter((F.col('date') >= split_date) & (F.col('date') < split_date2))

# ======================================================================

models_broadcasted = spark.sparkContext.broadcast(models)

def predict(row_iterator):
    X = []
    dates = []
    for i, row in enumerate(row_iterator):
        if i == 0:
            key = (row.shop, row.item)
        x = row.features.toArray() # may contain nan
        x[np.isnan(x)] = 0
        X.append(x)
        dates.append(row.date)
    if len(X) > 0:
        if key in models_broadcasted.value:
            pr = models_broadcasted.value[key].predict(X)
            yield (key, (pr, dates))
        else:
            yield (key, (None, None))

preds = dict(features_test.repartition(n_partitions, 'shop', 'item').rdd.mapPartitions(predict).collect())
# print(preds)

# ======================================================================

def y_true(row_iterator):
    y_true = []
    dates = []
     
    for i, row in enumerate(row_iterator):
        if i == 0:
            key = (row.shop, row.item)
        y_true.append(row.target)
        dates.append(row.date)
    if len(y_true) > 0:
        yield (key, (np.array(y_true), dates))

y_trueZ = dict(features_test.repartition(n_partitions, 'shop', 'item').rdd.mapPartitions(y_true).collect())
# print(y_trueZ)

# ======================================================================


print('\n'*10)


a = preds[key][0]
b = y_trueZ[key][0]

print
print('============================== predicted: ==============================')
print(a)
print('================================ true: =================================')
print(b)


mae = mean_absolute_error(a, b)
mse = mean_squared_error(a, b)
print('======================================================================')
print('MAE:', mae)
print('MSE:', mse)



print('\n'*10)
print('======================================================================')


plt.figure(figsize=(20, 4))
plt.plot(a, label='predict')
plt.plot(b, label='true')
plt.legend()
plt.xticks(np.arange(len(a)))


plt.title('MAE: {mae:.3f}       MSE: {mse:.3f}'.format(mae=mae, mse=mse))
plt.grid()
# display(plt.gcf())
# plt.show()
plt.savefig('prediction.png')

