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


# sales = spark.sql('select * from sales')
sales = spark.read.parquet('sales')








n_partitions = 15 # adjust it manually, it should be >= than n_shops * n_items






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

def make_features(df):
    # diff_intervals = np.geomspace(1, 512, 10).round().astype(int) # [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    diff_intervals = [1, 2, 10] # smaller for quick look  / debug
    


    # unboundedPreceding: -inf
    #  0: current row
    # -1: previous row

    cum_win = Window.partitionBy('shop', 'item').orderBy('date').rangeBetween(Window.unboundedPreceding, 0)
    features = df                                                   \
        .withColumn('cum_sum'  , F.sum      ('sale').over(cum_win)) \
        .withColumn('cum_mean' , F.mean     ('sale').over(cum_win)) \
        .withColumn('cum_min'  , F.min      ('sale').over(cum_win)) \
        .withColumn('cum_max'  , F.max      ('sale').over(cum_win)) \
        .withColumn('cum_std'  , F.stddev   ('sale').over(cum_win)) \
        .withColumn('cum_var'  , F.variance ('sale').over(cum_win)) \
        .withColumn('cum_skew' , F.skewness ('sale').over(cum_win)) \
        .withColumn('cum_kurt' , F.kurtosis ('sale').over(cum_win))
    # features.show()
    
    # note: lag function can have default value

    for d in diff_intervals:
        print(d)
        lag_win = Window.partitionBy('shop', 'item').orderBy('date')
        features = features                                                   \
            .withColumn(str(d) + '_day_lag' , F.lag('sale', d).over(lag_win))
    lag_win = Window.partitionBy('shop', 'item').orderBy('date')
    features = features.withColumn('target', F.lag('sale', -28).over(lag_win)) # target is 28 day in the future
    # features.show()
    
    days = lambda i: i * 86400 

    # diff_intervals = np.geomspace(1, 512, 10).round().astype(int) # [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    diff_intervals = [1, 2, 10, 100] # smaller for quick look  / debug

    for d in diff_intervals:
        # weird cast, see 
        # https://stackoverflow.com/a/33226511/4204843
        # lag_win = Window.partitionBy('shop', 'item').orderBy('date').rangeBetween(-days(1), 0)
        lag_win = Window.partitionBy('shop', 'item').orderBy(F.col('date').cast('timestamp').cast('long')).rangeBetween(-days(d), 0)
        features = features                                                 \
            .withColumn(str(d) + '_day_rolling_sum'  , F.sum      ('sale').over(lag_win)) \
            .withColumn(str(d) + '_day_rolling_mean' , F.mean     ('sale').over(lag_win)) \
            .withColumn(str(d) + '_day_rolling_min'  , F.min      ('sale').over(lag_win)) \
            .withColumn(str(d) + '_day_rolling_max'  , F.max      ('sale').over(lag_win)) \
            .withColumn(str(d) + '_day_rolling_std'  , F.stddev   ('sale').over(lag_win)) \
            .withColumn(str(d) + '_day_rolling_var'  , F.variance ('sale').over(lag_win)) \
            .withColumn(str(d) + '_day_rolling_skew' , F.skewness ('sale').over(lag_win)) \
            .withColumn(str(d) + '_day_rolling_kurt' , F.kurtosis ('sale').over(lag_win))
    features.show()
    
    feature_columns = []
    for col in features.columns:
        if col not in ('date', 'shop', 'item', 'sale', 'target'):
            print(col)
            mean = features.agg({col: 'mean'}).first()[0]
            if mean is None:
                mean = 0. # dirty fix
            print(mean, type(mean))
            features = features.fillna(mean, subset=col)
            feature_columns.append(col)
    print(feature_columns)

    features = VectorAssembler(
        inputCols = feature_columns, 
        outputCol = 'features'
    ).transform(features) # add column 'features'
    features.show()

    return features, feature_columns

# ======================================================================

features, feature_columns = make_features(sales)
features_train = features.filter(sales['date'] < split_date)

# ======================================================================

from pyspark.ml.linalg import DenseVector 

def fit(row_iterator):
    X = []
    y = []
    
    for i, row in enumerate(row_iterator):
        if i == 0:
            key = (row.shop, row.item)
        x = row.features.toArray() # may contain nan
        x[np.isnan(x)] = 0
        X.append(x)
        y.append(row.target)
    
    if len(X) > 0:
        lr = LinearRegression().fit(X, y)
        yield (key, lr)

models = dict(features_train.repartition(n_partitions, 'shop', 'item').rdd.mapPartitions(fit).collect())
print(models)

import pickle
with open('models.pickle', 'wb') as handle:
    pickle.dump(models, handle, protocol=pickle.HIGHEST_PROTOCOL)
