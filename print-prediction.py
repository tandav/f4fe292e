import argparse
import sys
from pyspark.sql import SparkSession
import sys


parser = argparse.ArgumentParser()
parser.add_argument('table' , type=str                )
parser.add_argument('--shop', type=int, required=False)
parser.add_argument('--item', type=int, required=False)
args = parser.parse_args()

print(args)

if not args.table:
    print('error: table is not specified')
    sys.exit(1)

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet(args.table +'_prediction')


if args.shop and args.item:
    print('shop + item search')
    result = df.filter((df.shop == args.shop) & (df.item == args.item))
elif args.shop:
    print('shop search')
    result = df.filter(df.shop == args.shop)
elif args.item:
    print('item search')
    df.filter(df.item == args.item)
else:
    result = df

for row in result.collect():
    print(row.shop, row.item, end=' ')
    for p in row.prediction:
        print('{r:4}'.format(r=round(z)))
    print('\n')


# TODO
# add sum for each W1 W2 W3 W4 in report
