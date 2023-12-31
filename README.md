## Описание
Скрипт предназначен для получения произвольного набора данных из \
LogsAPI метрики и их последующей записи в БД MySQL https://yandex.ru/dev/metrika/doc/api2/logs/intro.html
В БД рекомендуется создать отдельную базу и пользователя для работы со скриптом.
Возможно получение данных за произвольный промежуток времени. 
В случае, если данные за период не помещаются в один отчет, скрипт разобьет временной промежуток на периоды по 15 дней
и будет выгружать данные в рамках нескольких отчетов. Логгирование реализовано выводом в терминал
подключение почты необходимо для получения оповещений об ошибках\готовности отчета.

## Начало работы

Для корректного разворачивания скрипта необходимо:
1. установить зависимости из файла requirements.txt\
`pip install -r requirements.txt`
2. создать файл окружения .env в котором прописать следующие доступы\

| Имя переменной | Описание                                                    | Тип данных |
|----------------|-------------------------------------------------------------|------------|
| MS_USERNAME    | Имя пользователя базы данных MySQL                          | str        |
| MS_PASSWORD    | Пароль пользователя базы данных MySQL                       | str        |
| MS_HOST        | url\ip где располагается база данных MySQL                  | str        |
| MS_PORT        | Порт по которому доступна база данных MySQL.                | int        |
| MS_BD          | Наименование базы данных в которую будет произведена запись | str        |
| YA_PASS        | Пароль для доступа к яндекс почти по SMTP                   | str        |
| YA_LOGIN       | Логин от яндекс почты. Ввод тестировался без @              | str        |
| YA_HOST        | Хост SMTP яндекса, обычно принимает значение smtp.yandex.ru | str        |
| YA_FROM        | Указать you_username@yandex.ru                              | str        |
| YM_TOKEN       | Токен яндекс метрики                                        |            |

3. в файле bd_config.py указать полный путь до .env файла
4. согласно инструкции в config_file.py указать: \
- номер счетчика метрики
- тип выгружаемого отчета
- данные, которые необходимо выгрузить
- период выгрузки
- наименование таблицы, которая будет создана в БД MySQL
- почта, на которую должны прийти уведомления
5. запустить get_data.py
