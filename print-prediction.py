import argparse
import sys
from pyspark.sql import SparkSession
import sys


parser = argparse.ArgumentParser()
parser.add_argument('table' , type=str                )
parser.add_argument('--shop', type=int, required=False)
parser.add_argument('--item', type=int, required=False)
parser.add_argument('--file', type=str, required=False)
args = parser.parse_args()



if not args.table:
    print('error: table is not specified')
    sys.exit(1)

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet(args.table +'_prediction')


if args.shop and args.item:
    result = df.filter((df.shop == args.shop) & (df.item == args.item))
elif args.shop:
    result = df.filter(df.shop == args.shop)
elif args.item:
    df.filter(df.item == args.item)
else:
    result = df

report  = ''
for row in result.collect():
    report += 'shop: ' + str(row.shop) + ' item: ' + str(row.item) + '   '
    report += 'sales prediction for next 4 weeks:\n'

    for week in range(4):
        report += 'week' + str(week) + ': '
        week_pred = row.prediction[week * 7 : (week + 1) * 7]
        week_pred_round = [round(p) for p in week_pred]
        
        for p in week_pred_round:
            report += str(p).rjust(3) + ' '
        report += '    week sum: ' + str(sum(week_pred_round)) + '\n'
    report += '\n'


def gprint(*args):
    print('\033[32;1m', end='') # GREEN
    print(*args, end='\033[0m\n')

gprint(report)

if args.file:
    with open(args.file, 'w') as f:
        f.write(report)
