import pandas as pd
import numpy as np
from pyspark.sql.types import *
import datetime
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error
import sys

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


# sales = spark.sql('select * from sales')

train_table = sys.argv[1]
sales = spark.read.parquet(train_table)



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
            feature_columns.append(col)
    print(feature_columns)

    # feature_columns = []
    # for col in features.columns:
    #     if col not in ('date', 'shop', 'item', 'sale', 'target'):
    #         print(col)
    #         mean = features.agg({col: 'mean'}).first()[0]
    #         if mean is None:
    #             mean = 0. # dirty fix
    #         print(mean, type(mean))
    #         features = features.fillna(mean, subset=col)
    #         feature_columns.append(col)
    # print(feature_columns)

    sql_query = ['date', 'shop', 'item', 'sale', 'target']


    # replace null and NaN 
    sql_query += [
        F.when( F.isnan(features[c]) | F.isnull(features[c]), 1).otherwise(features[c]).alias(c)
        for c in feature_columns
    ]
    
    features = features.select(sql_query)
    
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


def fit(group_iterator):
    X, y  = [], []
    for row in group_iterator:
        X.append(row.features.toArray())
        y.append(row.target)
    return LinearRegression().fit(X, y)

models = dict(features_train.rdd.keyBy(lambda x: (x.shop, x.item)).groupByKey().mapValues(fit).collect())
print(models)

import pickle
with open('models.pickle', 'wb') as handle:
    pickle.dump(models, handle, protocol=pickle.HIGHEST_PROTOCOL)

# features.write.save('features', format='parquet', mode='overwrite')

