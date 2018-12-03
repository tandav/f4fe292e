import pickle
import sys

with open('models.pickle', 'rb') as handle:
    models = pickle.load(handle)


def predict_by_key(key):
    if key in models:
        return models[key].predict(X)
    else:
        return str(key) + 'not in models list'

if len(sys.argv) == 2:   # from file
    with open(sys.argv[1]) as f:
        for line in f:
            s = line.split()
            key = (int(s[0]), int(s[1]))
            print(predict_by_key(key))
elif len(sys.argv) == 3: # from args
    shop = int(sys.argv[1])
    item = int(sys.argv[2])
    key = (shop, item)
    print(predict_by_key(key))
else:
    print('pass shop and item to predict sales')
    print('you can pass file or 2 numbers')
    sys.exit(1)
