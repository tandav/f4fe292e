from pyspark.ml.feature import VectorAssembler
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import *


def make_features(df):

    # ==================== add missing days with 0 sales =====================
    
    schema = StructType([
        StructField(name = 'date', dataType = DateType()   , nullable=False),
        StructField(name = 'shop', dataType = LongType()   , nullable=False),
        StructField(name = 'item', dataType = LongType()   , nullable=False),
        StructField(name = 'sale', dataType = IntegerType(), nullable=False),
    ])

    dates_unique = df.rdd.map(lambda x: x.date).distinct()
    shops_unique = df.rdd.map(lambda x: x.shop).distinct()
    items_unique = df.rdd.map(lambda x: x.item).distinct()

    df_keyed = df.rdd.map(lambda x: ((x.date, x.shop, x.item), x.sale))

    features = dates_unique                              \
        .cartesian( 
            shops_unique.cartesian(items_unique)
        )                                                \
        .map(lambda x: ( ((x[0], x[1][0], x[1][1])), 0)) \
        .subtractByKey(df_keyed)                         \
        .union(df_keyed)                                 \
        .map(lambda x: Row(
            date=x[0][0], 
            shop=x[0][1], 
            item=x[0][2], 
            sale=x[1])
        )                                                \
        .sortBy(lambda x: x.date)                        \
        .map(lambda x: (x.date, x.shop, x.item, x.sale)) \
        .toDF(schema)

    # .map(lambda x: (x[0][0], x[0][1], x[0][2], x[1]))    \
    
    # ======================= add cumulative features ========================

    # unboundedPreceding: -inf
    #  0: current row
    # -1: previous row

    cum_win = Window.partitionBy('shop', 'item').orderBy('date').rangeBetween(Window.unboundedPreceding, 0)
    features = features                                                   \
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

    # =========================== add lag features ===========================


    # diff_intervals = np.geomspace(1, 512, 10).round().astype(int) # [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    diff_intervals = [1, 2, 10] # smaller for quick look  / debug


    for d in diff_intervals:
        print(d)
        lag_win = Window.partitionBy('shop', 'item').orderBy('date')
        features = features                                                   \
            .withColumn(str(d) + '_day_lag' , F.lag('sale', d).over(lag_win))
    # features.show()
    

    # ========================= add rolling features =========================

    days = lambda i: i * 86400 

    for d in diff_intervals:
        # weird cast, see 
        # https://stackoverflow.com/a/33226511/4204843
        # lag_win = Window.partitionBy('shop', 'item').orderBy('date').rangeBetween(-days(1), 0)
        lag_win = Window.partitionBy('shop', 'item').orderBy(F.col('date').cast('timestamp').cast('long')).rangeBetween(-days(d), 0)
        features = features                                                               \
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
        if col not in ('date', 'shop', 'item', 'sale'):
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

    # ===================== replace null and NaN with 1 ======================
   
    sql_query  = ['date', 'shop', 'item', 'sale']
    sql_query += [
        F.when( F.isnan(features[c]) | F.isnull(features[c]), 1).otherwise(features[c]).alias(c)
        for c in feature_columns
    ]
    features = features.select(sql_query)

    # ====================== add features vector column ======================

    features = VectorAssembler(
        inputCols = feature_columns, 
        outputCol = 'features'
    ).transform(features) # add column 'features'
    # features.show()

    return features


def make_target(df):
    target_win = Window.partitionBy('shop', 'item').orderBy('date')
    return df.withColumn('target', F.lag('sale', -28).over(target_win)) # target is sales in 28 day in the future
