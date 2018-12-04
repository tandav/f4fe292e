import datetime
from pyspark.sql import Window
from sklearn.linear_model import LinearRegression
from pyspark.sql.types import *
import sys
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from helpers import make_features, make_target

# train_data = spark.sql('select * from sales')
train_table = sys.argv[1]
# train_table = 'sales'
train_data = spark.read.parquet(train_table)

# ======================================================================

train_data_features        = make_features(train_data)
train_data_features_target = make_target  (train_data_features)

# ======================================================================

def fit_predict(group_iterator):
    X_train   = []
    y_train   = []
    X_predict = []

    for row in group_iterator:
        if row.target is not None:
            X_train.append(row.features.toArray())
            y_train.append(row.target)
        else:
            X_predict.append(row.features.toArray())
    
    return LinearRegression().fit(X_train, y_train).predict(X_predict).tolist()


schema = StructType([
    StructField(name = 'shop'      , dataType = LongType()                                             , nullable=False),
    StructField(name = 'item'      , dataType = LongType()                                             , nullable=False),
    StructField(name = 'prediction', dataType = ArrayType(elementType=DoubleType(), containsNull=False), nullable=False),
])

prediction = train_data_features_target      \
    .rdd.keyBy(lambda x: (x.shop, x.item))   \
    .groupByKey()                            \
    .mapValues(fit_predict)                  \
    .map(lambda x: (x[0][0], x[0][1], x[1])) \
    .toDF(schema)

prediction.write.save(train_table + '_prediction', format='parquet', mode='overwrite')
