import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error
from pyspark.sql import SparkSession
import sys
from helpers import add_missing, add_target

spark = SparkSession.builder.getOrCreate()

if len(sys.argv) < 3:
    print('pass prediction table and true table')
    sys.exit(1)

pred_table = sys.argv[1]
true_table = sys.argv[2]

pred = spark.read.parquet(pred_table)
pred = pred.rdd \
    .map(lambda x: ((x.shop, x.item), x.prediction)) \
    .collect()
pred = dict(pred)

true = spark.read.parquet(true_table)
true = add_target(add_missing(true))
true = true                                                \
    .rdd.keyBy(lambda x: (x.shop, x.item))                 \
    .groupByKey()                                          \
    .mapValues(lambda x: [z.target for z in list(x)[:28]]) \
    .collect()
true = dict(true)

mae_mean = 0
mse_mean = 0
n = 0

for key in pred.keys():
    print('shop {shop}  item {item}   '.format(shop=key[0], item=key[1]), end=' ')
    p = pred[key]
    t = true[key]
    mae = mean_absolute_error(p, t)
    mse = mean_squared_error (p, t)
    mae_mean += mae
    mse_mean += mse
    n += 1

    mae = '{mae:6.2f}'.format(mae=mae)
    mse = '{mse:6.2f}'.format(mse=mse)

    print(' MAE:', mae, '   MSE: ', mse)


mae_mean /= n
mse_mean /= n
mae_mean = '{mae_mean:5.2f}'.format(mae_mean=mae_mean)
mse_mean = '{mse_mean:6.2f}'.format(mse_mean=mse_mean)
print('-'*46)
print('                  mean: ', mae_mean, '  mean: ', mse_mean)
