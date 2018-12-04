сгенерировать рандомные данные  
1000 дней продаж начиная с `2015-03-21`  
сохранить результат в таблицу `sales`

```sh
spark-submit generate-data.py '2015-03-21' 1000 sales
```

> примечание: последняя дата будет `2017-12-14`  

---

сделать предсказания на 28 дней в будущее  
для каждой уникальной пары `(shop, item)`  
в таблице `sales`

```sh
spark-submit predict.py sales
```

результаты сохраняются в таблицу `sales_prediction`


---

просмотреть результаты предсказания для таблицы sales ([пример вывода](prediction.txt))

```sh
spark-submit print-prediction.py sales                   # all results
spark-submit print-prediction.py sales --shop=1 --item=2 # results for shop 1 and item 2
spark-submit print-prediction.py sales --shop=0          # results for shop 0
spark-submit print-prediction.py sales --item=2          # results for item 2
```

