import os

from api_metrika import Counter
from sqlalchemy import inspect
import mssql
import config_file
import bd_config
import general
from loguru import logger
from dotenv import load_dotenv


load_dotenv(bd_config.PATH_ENV)

def main():
    logger.info('Start script')
    # корректирует отображение pandas dataframe в консоли
    general.pretty_print()

    # создаем подключение к БД
    engine = mssql.create_engine(os.environ.get('MS_USERNAME'),
                                 os.environ.get('MS_PASSWORD'),
                                 os.environ.get('MS_HOST'),
                                 os.environ.get('MS_BD'))
    logger.info('Engine create')

    # получаем список существующих таблиц
    insp = inspect(engine)
    # проверяем таблицу на существование
    if config_file.table_name in insp.get_table_names():
        logger.warning('Table already exists.')
        subject = 'MetrikaLogsAPI. Ошибка создания таблицы.'
        text = f'Таблица {config_file.table_name} уже существуент в БД {bd_config.DATABASE_METRIKA_RAW_DATA}.\n' \
               f'Переименуйте таблицу.'
        general.send_mail(bd_config.HOST, bd_config.FROM, config_file.w_email, subject, text)
        exit()

    # создаем объект подключения к yandex metrika
    ym_counter = Counter(os.environ.get('YM_TOKEN'), config_file.counter)

    # подготавливает отчет
    ym_counter.get_report_bulk_insert(engine,
                                      config_file.type_report,
                                      config_file.counter,
                                      config_file.start_date,
                                      config_file.end_date,
                                      config_file.params_list,
                                      config_file.table_name,
                                      config_file.w_email)

    subject = 'MetrikaLogsAPI. Выгрузка данных завершена.'
    text = f'Выгрузка данных завершена.\n' \
           f'Информация находится в таблице {config_file.table_name} на сервере {bd_config.SERVER}'
    general.send_mail(bd_config.HOST, bd_config.FROM, config_file.w_email, subject, text)
    logger.info('Сompleted successfully!')

'''def delete_all_logs(counter):
    logger.info('Start script')
    # корректирует отображение pandas dataframe в консоли
    general.pretty_print()
    # подключение к прокси MF
    general.enable_proxy_tech(bd_config.PROXY_LOGIN,
                              bd_config.PROXY_PASS)
    ym_counter = Counter(bd_config.CLIENT_ID, config_file.counter)
    id_rep = ym_counter.all_report_id()
    for i in id_rep:
        ym_counter.del_report(i)
        logger.info(f'Delete log id  {i}')
    logger.info('Сompleted successfully!')'''


if __name__ == '__main__':
    try:
        main()
    except ValueError as err:
        subject = 'MetrikaLogsAPI. Download failed'
        text = f'Выгрузка отчета завершилась ошибкой.\n' \
               f'{err}'
