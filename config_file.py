"""
Данный файл является файлом конфигурации для выгрузки разового отчета сырых данных из LogsAPI Яндекс Метрики.
Для корректной работы скрипта, каждая из переменных должна содержать данные,
в противном случае выгрузка завершится ошибкой.
"""

""""
Номер счетчика Яндекс Метрики
"""
counter = '66217906'  # сквозной счетчик 26851704

"""
Тип отчета. 
Всего согласно документации возможно формирование двух типов отчета:
- visits (визиты)
- hits (просмотры)
https://yandex.ru/dev/metrika/doc/api2/logs/queries/createlogrequest.html
"""
type_report = 'hits'

"""
В зависимости от выбранного типа отчета (visits/hits) меняется набор используемых параметров в запросе:
- visits
https://yandex.ru/dev/metrika/doc/api2/logs/fields/visits.html
- hits
https://yandex.ru/dev/metrika/doc/api2/logs/fields/hits.html

Параметры должны быть переданы в формате list, пример ниже
"""
params_list = ["ym:pv:date",
               "ym:pv:clientID",
               "ym:pv:watchID",            
               "ym:pv:URL"
               ]


"""
Дата начала/дата окончания в формате YYYY-MM-DD
"""
start_date = '2023-03-01'
end_date = '2023-03-15'

"""
Указываем наименование таблицы в БД MetrikaRawData, куда будут помещены полученные данные
"""
table_name = 'test_write_data1'

"""
Рабочая почта
"""

w_email = 'godmargon@yandex.ru'
