import pandas as pd
import numpy as np
from pyspark.sql.types import *
import datetime
from pyspark.sql import functions as F
from sklearn.metrics import mean_absolute_error, mean_squared_error
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
# features = spark.read.parquet('features')



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
    sales_pred = models_broadcasted.value[key].predict(features)
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
    mae_mean += mae
    mse_mean += mse
    n += 1

mae_mean /= n
mse_mean /= n
print('======================================================================')
print('MAE_mean:', mae_mean, 'MSE_mean:', mse_mean)

print('\n'*10)
print('======================================================================')



