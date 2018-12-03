сгенерировать рандомные данные
```sh
spark-submit generate-data.py
```


обучить модели
```sh
spark-submit train.py
```


сделать предсказания на 28 для `shop = 1`  and `item = 2`
```sh
spark-submit predict.py 1 2
```

либо
```sh
spark-submit predict.py to_predict.txt
```

передаваемый файл должен иметь [такую](to_predict.txt) структуру. Первая колонка `shop`, вторая `item`

