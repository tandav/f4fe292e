import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument('table' , type=str                )
parser.add_argument('--shop', type=int, required=False)
parser.add_argument('--item', type=int, required=False)
args = parser.parse_args()

print(args)


if 


# if args.table:
    # print('all results')

# if args.a == 'magic.name':
    # print 'You nailed it!'

# with open('models.pickle', 'rb') as handle:
#     models = pickle.load(handle)


# def predict_by_key(key):
#     if key in models:
#         return models[key].predict(X)
#     else:
#         return str(key) + ' not in models list'

# if len(sys.argv) == 2:   # from file
#     if sys.argv[1] == '-a':
#         for key in models.keys():
#             print(predict_by_key(key))
#     else:
#         with open(sys.argv[1]) as f:
#             for line in f:
#                 s = line.split()
#                 key = (int(s[0]), int(s[1]))
#                 print(predict_by_key(key))
# elif len(sys.argv) == 3: # from args
#     shop = int(sys.argv[1])
#     item = int(sys.argv[2])
#     key = (shop, item)
#     print(predict_by_key(key))
# else:
#     print('pass shop and item to predict sales')
#     print('you can pass file or 2 numbers')
#     sys.exit(1)
