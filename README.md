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

просмотреть результаты предсказания для таблицы sales 

```sh
spark-submit print-prediction.py sales                   # all results
spark-submit print-prediction.py sales --shop=1 --item=2 # results for shop 1 and item 2
spark-submit print-prediction.py sales --shop=0          # results for shop 0
spark-submit print-prediction.py sales --item=2          # results for item 2
spark-submit print-prediction.py sales --file=report.txt # save report to file
```
[пример вывода](prediction.txt)

---

### тестирование модели

сгенерируем данных еще на 56 дней в будущее по отноошению к прошлой таблице и сохраним в таблицу `sales_true`  

```sh
spark-submit generate-data.py '2017-12-15' 56 sales_true
```

28 + 28 = 56  

для первых 28 дней можно увидеть продажи через 28 дней (посмотреть во вторых 28 днях). Это target для модели.  

Сравним предсказания обученной ранее модели c реальными значениями продаж:  

```sh
spark-submit test.py sales_prediction sales_true
```

пример результатов тестирования
