# Решение тестового задания Ростелеком
##  Особенности решения
Как скрипт реализованный на чистом python, так и с использованием pyspark. Не нуждаются в конфигурировании источника.

Для указанного апи набор типов данных достаточно тривиален, из за чего принял решение, реализовать универсальный загрузчик для данного типа api. 

 #№  Pure Python
 На чистом питоне использовал базовую библиотеку для работы с postgres, попытки обернуть реализацию в orm посчитал избыточными для данной задачи.

 ## Pyspark 
 В случае спарка полагаюсь на базовое поведение режима append, для dataframeWriter, который в любом случае создает таблицу. 

### P.S. Приношу свои извинения за такую задержку в ответе и слабую реализацию. 
### На текущем рабочем месте, необходимо в срочном порядке править бизнес-функционал.
