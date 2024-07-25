# Вывод с Jupyter Notebook

```python
from main import show_df, create_spark_session

spark_session = create_spark_session()

show_df(spark_session)
```

    Продукты:
    +---+-------+
    | id|product|
    +---+-------+
    |  1|   milk|
    |  2|   meat|
    |  3|  bread|
    +---+-------+
    
    Категории:
    +--------+---+
    |category| id|
    +--------+---+
    |     eat|  1|
    |   drink|  2|
    |  health|  3|
    +--------+---+
    
    Результат:
    +-------+--------+
    |product|category|
    +-------+--------+
    |  bread|    NULL|
    |   milk|     eat|
    |   meat|     eat|
    |   milk|  health|
    |   meat|  health|
    |   milk|   drink|
    +-------+--------+
```python
