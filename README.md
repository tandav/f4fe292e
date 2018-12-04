сгенерировать случайные данные  
1000 дней продаж начиная с `2015-03-21`  
количество магазинов 3  
количество продуктов 4  

сохранить результат в таблицу `sales`

```sh
spark-submit generate-data.py '2015-03-21' 1000 --n_shops=3 --n_items=4 sales
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
[пример вывода](report.txt)

---

### тестирование модели

сгенерируем данных еще на 56 дней в будущее по отноошению к прошлой таблице и сохраним в таблицу `sales_true`  
(будет использоваться тот же генератор, поэтому ряды продолжаться)  

```sh
spark-submit generate-data.py '2017-12-15' 56 --n_shops=3 --n_items=4 sales_true
```

28 + 28 = 56  

для первых 28 дней можно увидеть продажи через 28 дней (посмотреть во вторых 28 днях). Это target для модели.  

Сравним предсказания обученной ранее модели c реальными значениями продаж:  

```sh
spark-submit test.py sales_prediction sales_true
```

пример результатов тестирования:

```
shop 0  item 1     MAE:   8.90    MSE:  105.28
shop 1  item 2     MAE:   8.80    MSE:  105.32
shop 1  item 3     MAE:   1.09    MSE:    1.60
shop 0  item 3     MAE:   5.01    MSE:   33.36
shop 1  item 1     MAE:   0.00    MSE:    0.00
shop 2  item 0     MAE:   1.14    MSE:    1.69
shop 0  item 0     MAE:   1.95    MSE:    5.22
shop 2  item 3     MAE:   9.74    MSE:  122.64
shop 2  item 1     MAE:   2.18    MSE:    6.29
shop 2  item 2     MAE:  11.02    MSE:  151.59
shop 1  item 0     MAE:   7.60    MSE:   78.29
shop 0  item 2     MAE:   9.37    MSE:  111.57
----------------------------------------------
                  mean:   5.57   mean:   60.24
```
