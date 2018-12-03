---

сгенерировать рандомные данные  
1000 дней начиная с `2015-03-21`  
сохранить результат в таблицу `sales`

```sh
spark-submit generate-data.py '2015-03-21' 1000 sales
```

примечание: последняя строка будет иметь дату `2017-12-15`

---

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

передаваемый файл должен иметь [такую](to_predict.txt) структуру.  
Первая колонка `shop`, вторая `item`  
название файла не важно

