import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error
from pyspark.sql import SparkSession
import sys
from helpers import add_missing, add_target

spark = SparkSession.builder.getOrCreate()

if len(sys.argv) < 3:
    print('pass prediction table and true table')
    sys.exit(1)

def gprint(*args):
    print('\033[32;1m', end='') # GREEN
    print(*args, end='\033[0m\n')

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

print(pred)
print(true)

mae_mean = 0
mse_mean = 0
n = 0

for key in pred.keys():
    print('shop: {shop}, item: {item}'.format(shop=key[0], item=key[1]), end=' ')
    p = pred[key]
    t = true[key]
    mae = mean_absolute_error(p, t)
    mse = mean_squared_error (p, t)
    print('MAE:', mae, 'MSE:', mse)
    mae_mean += mae
    mse_mean += mse
    n += 1

mae_mean /= n
mse_mean /= n

print('MAE_mean:', mae_mean, 'MSE_mean:', mse_mean)




# print('======================================================================')

# print('\n'*10)
# print('======================================================================')






# # ======================================================================

# features_test = features.filter((F.col('date') >= split_date) & (F.col('date') < split_date2))

# # ======================================================================

# models_broadcasted = spark.sparkContext.broadcast(models)

# def test(group_iterator):
#     features   = []
#     sales_pred = []
#     sales_true = []

#     first = True
#     for row in group_iterator:
#         if first:
#             key = (row.shop, row.item)
#             first = False
#         features.append(row.features.toArray())
#         sales_true.append(row.target)
#     sales_pred = models_broadcasted.value[key].predict(features)
#     sales_true = np.array(sales_true)
#     return {'sales_pred': sales_pred, 'sales_true': sales_true}

# pred_true = features_test.rdd.keyBy(lambda x: (x.shop, x.item)).groupByKey().mapValues(test).collect()

