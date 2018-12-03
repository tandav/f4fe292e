import pandas as pd
import numpy as np
from pyspark.sql.types import *
import datetime
from pyspark.sql import functions as F
from sklearn.metrics import mean_absolute_error, mean_squared_error
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
features = spark.read.parquet('features')

import pickle
with open('models.pickle', 'rb') as handle:
    models = pickle.load(handle)


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

def test(group_iterator):
    features   = []
    sales_pred = []
    sales_true = []

    first = True
    for row in group_iterator:
            if first:
                key = (row.shop, row.item)
                first = False
        features.append(row.features.toArray())
        sales_true.append(row.target)
    sales_pred = models_broadcasted.value[key].predict(X)
    sales_true = np.array(sales_true)
    return {'sales_pred': sales_pred, 'sales_true': sales_true}

pred_true = features_test.rdd.keyBy(lambda x: (x.shop, x.item)).groupByKey().mapValues(test).collect()

print('======================================================================')
print('\n'*10)

mae_mean = 0
mse_mean = 0
n = 0

for key, p in pred_true:
    mae = mean_absolute_error(p['sales_pred'], p['sales_true'])
    mse = mean_squared_error (p['sales_pred'], p['sales_true'])
    print(key, 'MAE:', mae, 'MSE:', mse)
    mae_sum += mae
    mse_sum += mse
    n += 1

mae_mean /= n
mse_mean /= n
print('======================================================================')
print(key, 'MAE_mean:', mae_mean, 'MSE_mean:', mse_mean)

print('\n'*10)
print('======================================================================')



