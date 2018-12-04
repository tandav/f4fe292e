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
    print('shop', row.shop, 'item', row.item)

    print('sales prediction for next 4 weeks:')

    for week in range(4):
        print('week', week, end=' ')
        week_pred = row.prediction[week * 7 : (week + 1) * 7]
        week_pred_round = [round(p) for p in week_pred]
        
        for p in week_pred_round:
            print(str(p).rjust(3), end=' ')
        print('week sum:', sum(week_pred_round))
    print()
    # rounded = [round(p) for p in row.prediction]
    # rounded_str = []
    # print(row.shop, row.item, end=' ')
    # for p in row.prediction:
        # print('{r:3}'.format(r=round(p)), end=' ')
    # print()


# TODO
# add sum for each W1 W2 W3 W4 in report
