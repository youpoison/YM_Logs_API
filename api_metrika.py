from requests.exceptions import ChunkedEncodingError
from tapi_yandex_metrika import YandexMetrikaLogsapi
import general
import bd_config
import pandas as pd
from loguru import logger
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv


load_dotenv(bd_config.PATH_ENV)

class Counter:

    def __init__(self, token, counter_id):
        self.token = token
        self.counter_id = counter_id
        self.client = self.client_counter()

    def client_counter(self):
        '''
        Создает объект подключения, с помощью которого производятся все операции
        :return:
        Объект подключения
        '''
        client = YandexMetrikaLogsapi(
            access_token=self.token,
            default_url_params={'counterId': self.counter_id}
        )
        logger.info('Client create')
        return client

    def all_report_id(self):
        '''Возвращает id существующих отчетов logs_api для определенного счетчика
        :return:
        id отчетов в формате list
        '''
        result = self.client.allinfo().get()
        request_id = []
        for i in result['requests']:
            request_id.append(i['request_id'])
        logger.info('Report IDs received')
        return request_id

    def del_report(self, request_id):
        '''Удаляет отчет по request_id в logs_api

        :param request_id: string
        :return:
        Сообщение об успешном удалении
        '''
        result = self.client.clean(requestId=request_id).post()
        logger.warning('Отчет успешно удален')
        return result['log_request']['status']

    def del_report_new(self, request_id: str):
        '''Удаляет отчет по request_id в logs_api

        :param request_id: string
        :return:
        Сообщение об успешном удалении
        '''
        stat_response = 'timeout'
        while stat_response == 'timeout':
            try:
                result = self.client.clean(requestId=request_id).post()
                logger.warning(f'Отчет {request_id} удален из LOGS_API')
                stat_response = 'good'
                return result['log_request']['status']
            except ChunkedEncodingError as e:
                logger.warning('Response from LogsAPI timeout.')
                stat_response = 'timeout'
            except Exception as err:
                dict_result = {
                    'status': 'err',
                    'error_text': err
                }
                logger.error(err)
                return dict_result
        dict_result = {
            'status': 'ok'
        }
        return dict_result

    def create_report(self, params: dict):
        """
        Creates a report on the logs API side. Returns the id of the created report
        :param params: dictionary of parameters
        :return: str
        """
        # post запрос на создание отчета
        stat_response = 'timeout'
        while stat_response == 'timeout':
            try:
                order = self.client.create().post(params=params)
                stat_response = 'good'
                # получение id запроса
                request_id = order['log_request']['request_id']
                logger.info(f'Id созданного отчета {request_id}')
            except ChunkedEncodingError as e:
                logger.warning('Response from LogsAPI timeout.')
                stat_response = 'timeout'
            except Exception as err:
                dict_result = {
                    'status': 'err',
                    'request_id': None,
                    'error_text': err
                }
                logger.error(err)
                return dict_result

        dict_result = {
            'status': 'ok',
            'request_id': request_id
        }
        return dict_result

    def check_status_parallel(self, request_id: str):
        """
        Polls logs API every 10 seconds, returns the result of the survey.
        If it receives a processing_failed error, it shuts down.
        :param request_id: id of the report in the logs API
        :return: dict
        """
        # получение информации о статусе готовности отчета
        stat_response = 'timeout'
        while stat_response == 'timeout':
            try:
                info = self.client.info(requestId=request_id).get()
                info_status = info['log_request']['status']
                stat_response = 'good'
            except ChunkedEncodingError as e:
                logger.warning('Response from LogsAPI timeout.')
                stat_response = 'timeout'
            except Exception:
                return True
        return info_status

    def load_report(self, request_id: str):
        """
        unloads the report, if we get a timeout proxy, tries to unload it again
        :param request_id: id of the created report in the logs API
        :return: pandas.DataFrame
        """
        report_df = pd.DataFrame()
        stat_response = 'timeout'
        while stat_response == 'timeout':
            try:
                report = self.client.download(requestId=request_id).get()
                stat_response = 'good'
            except ChunkedEncodingError as e:
                logger.warning('Response from LogsAPI timeout.')
                stat_response = 'timeout'
            except Exception as err:
                dict_result = {
                    'status': 'err',
                    'report_df': None,
                    'error_text': err
                }
                logger.error(err)
                return dict_result
        part_counter = 0
        for part in report().parts():
            logger.info(f"Part {part_counter} processed")
            stat_response = 'timeout'
            while stat_response == 'timeout':
                try:
                    part_df = pd.DataFrame.from_dict(part().to_dicts())
                    report_df = pd.concat([report_df, part_df])
                    stat_response = 'good'
                except ChunkedEncodingError as e:
                    logger.warning('Response from LogsAPI timeout.')
                    stat_response = 'timeout'
                except Exception as err:
                    dict_result = {
                        'status': 'err',
                        'report_df': None,
                        'error_text': err
                    }
                    logger.error(err)
                    return dict_result
            part_counter += 1
        dict_result = {
            'status': 'ok',
            'report_df': report_df
        }
        return dict_result

    def get_report_bulk_insert(self, engine, type_report, counter, start_date, end_date, fields, table_name, w_email):

        # формируется словарь параметров
        params = {
            "fields": f'{",".join(str(e) for e in fields)}',  # преобразование list в string
            "source": type_report,
            "date1": start_date,
            "date2": end_date
        }
        logger.info('The list of parameters has been created')

        # проверка возможности создания отчета
        # https://yandex.ru/dev/metrika/doc/api2/logs/queries/evaluate.html
        possibility = self.client.evaluate().get(params=params)
        if possibility['log_request_evaluation']['possible']:
            # если создание отчета возможно
            logger.info('The report can be created')

            # post запрос на создание отчета
            try:
                order = self.client.create().post(params=params)
            except ValueError as err:
                subject = 'MetrikaLogsAPI. Download failed'
                text = f'Выгрузка отчета завершилась ошибкой.\n' \
                       f'{err}'
                general.send_mail(os.environ.get('YA_HOST'), os.environ.get('YA_FROM'), w_email, subject, text)
            # получение id запроса
            request_id = order['log_request']['request_id']
            logger.info(f'Id of the created report {request_id}')
            # получение информации о статусе готовности отчета
            info = self.client.info(requestId=request_id).get()

            # в цикле с паузой в 6 секунд проверяется статус готовности отчета
            # цикл завершится, если будет получен ответ processed
            while info['log_request']['status'] != 'processed':
                info = self.client.info(requestId=request_id).get()
                # если получен ответ processing_failed сообщает об этом, завершение скрипта
                if info['log_request']['status'] == 'processing_failed':
                    logger.error('Error: processing_failed')
                    subject = 'MetrikaLogsAPI. processing_failed.'
                    text = f'LogsAPI отдало ошибку processing_failed.\n' \
                           f'Отчет не может быть сформирован, запустите скрипт заново.\n' \
                           f'Если вы видите эту ошибку, просьба проинформировать о ней dmitry.kochnev@megafon.ru\n' \
                           f'Либо другого ответственного за работоспособность скрипта сотрудника.'
                    general.send_mail(os.environ.get('YA_HOST'), os.environ.get('YA_FROM'), w_email, subject, text)
                    self.del_report(request_id)
                    exit()
                time.sleep(6)
                logger.info(f"Preparation of the report. Status: {info['log_request']['status']}")

            logger.info('Report is ready. Unloading...')

            # скачивание первой части отчета
            report = self.client.download(requestId=request_id).get()
            # преобразование в pandas DataFrame
            report_df = pd.DataFrame.from_dict(report().to_dicts())
            logger.info(f"Report size {len(info['log_request']['parts'])} part")
            # проверка из скольки частей состоит отчет,
            # если всего одна часть - двигаемся дальше,
            # если частей несколько в цикле выкачиваем все части начиная со второй
            if len(info['log_request']['parts']) > 1:
                for i in info['log_request']['parts']:
                    if i['part_number'] == 0:
                        pass
                    else:
                        stat_response = 'timeout'
                        while stat_response == 'timeout':
                            try:
                                part = self.client.download(requestId=request_id,
                                                            partNumber=i['part_number']).get()
                                # преобразование в pandas DataFrame
                                part_df = pd.DataFrame.from_dict(part().to_dicts())
                                # объединяем с первой частью
                                report_df = pd.concat([report_df, part_df])
                                logger.info(f"Part {i['part_number']} processed")
                                stat_response = 'good'
                            except ChunkedEncodingError as e:
                                logger.warning('Response from LogsAPI timeout.')
                                stat_response = 'timeout'

            logger.info('Report success')
            logger.info('Writing to BD')

            report_df.to_sql(table_name,
                            con=engine,
                            # schema='dbo',
                            if_exists='fail',
                            index=False,
                            chunksize=20,
                            method='multi')

            # удаляем отчет из logs api
            self.del_report(request_id)
            logger.info('Done!')

        else:
            dt_start = datetime.strptime(start_date, '%Y-%m-%d')
            dt_end = datetime.strptime(end_date, '%Y-%m-%d')
            interval = 15
            period = dt_end - dt_start
            df_all_parts = pd.DataFrame()
            logger.info(f'The report is divided into {period.days // interval + 1} uploads.')
            for i in range(period.days // interval):
                logger.info(f'Part {i + 1} loading.')
                # формируется словарь параметров
                params = {
                    "fields": f'{",".join(str(e) for e in fields)}',  # преобразование list в string
                    "source": type_report,
                    "date1": (dt_start + timedelta(days=interval * i)).strftime('%Y-%m-%d'),
                    "date2": (dt_start + timedelta(days=interval * (i + 1)) - timedelta(days=1)).strftime('%Y-%m-%d')
                }
                print(params)
                # проверка возможности создания отчета
                # https://yandex.ru/dev/metrika/doc/api2/logs/queries/evaluate.html
                possibility = self.client.evaluate().get(params=params)
                print(possibility['log_request_evaluation']['possible'])
                if possibility['log_request_evaluation']['possible']:
                    # если создание отчета возможно
                    logger.info('The report can be created')

                    # post запрос на создание отчета
                    try:
                        order = self.client.create().post(params=params)
                    except ValueError as err:
                        subject = 'MetrikaLogsAPI. Download failed'
                        text = f'Выгрузка отчета завершилась ошибкой.\n' \
                               f'{err}'
                        general.send_mail(os.environ.get('YA_HOST'), os.environ.get('YA_FROM'), w_email, subject, text)
                    # получение id запроса
                    request_id = order['log_request']['request_id']
                    logger.info(f'Id of the created report {request_id}')
                    # получение информации о статусе готовности отчета
                    info = self.client.info(requestId=request_id).get()

                    # в цикле с паузой в 6 секунд проверяется статус готовности отчета
                    # цикл завершится, если будет получен ответ processed
                    while info['log_request']['status'] != 'processed':
                        info = self.client.info(requestId=request_id).get()
                        # если получен ответ processing_failed сообщает об этом, завершение скрипта
                        if info['log_request']['status'] == 'processing_failed':
                            logger.error('Error: processing_failed')
                            subject = 'MetrikaLogsAPI. processing_failed.'
                            text = f'LogsAPI отдало ошибку processing_failed.\n' \
                                   f'Отчет не может быть сформирован, запустите скрипт заново.\n' \
                                   f'Если вы видите эту ошибку, просьба проинформировать о ней dmitry.kochnev@megafon.ru\n' \
                                   f'Либо другого ответственного за работоспособность скрипта сотрудника.'
                            general.send_mail(os.environ.get('YA_HOST'), os.environ.get('YA_FROM'), w_email, subject, text)
                            self.del_report(request_id)
                            exit()
                        time.sleep(6)
                        logger.info(f"Preparation of the report. Status: {info['log_request']['status']}")

                    logger.info('Report is ready. Unloading...')

                    # скачивание первой части отчета
                    report = self.client.download(requestId=request_id).get()
                    # преобразование в pandas DataFrame
                    report_df = pd.DataFrame.from_dict(report().to_dicts())
                    logger.info(f"Report size {len(info['log_request']['parts'])} part")
                    # проверка из скольки частей состоит отчет,
                    # если всего одна часть - двигаемся дальше,
                    # если частей несколько в цикле выкачиваем все части начиная со второй
                    if len(info['log_request']['parts']) > 1:
                        for i in info['log_request']['parts']:
                            if i['part_number'] == 0:
                                pass
                            else:
                                stat_response = 'timeout'
                                while stat_response == 'timeout':
                                    try:
                                        part = self.client.download(requestId=request_id,
                                                                    partNumber=i['part_number']).get()
                                        # преобразование в pandas DataFrame
                                        part_df = pd.DataFrame.from_dict(part().to_dicts())
                                        # объединяем с первой частью
                                        report_df = pd.concat([report_df, part_df])
                                        logger.info(f"Part {i['part_number']} processed")
                                        stat_response = 'good'
                                    except ChunkedEncodingError as e:
                                        logger.warning('Response from LogsAPI timeout.')
                                        stat_response = 'timeout'

                    logger.info('Report success')
                    df_all_parts = pd.concat([df_all_parts, report_df])

                    # удаляем отчет из logs api
                    self.del_report(request_id)


                else:
                    logger.error(f'It is necessary to reduce the interval. '
                                 f'Now the interval is equal to {interval} days')
                    subject = 'MetrikaLogsAPI. Interval failed.'
                    text = f'Отчет не может быть сформирован, необходимо уменьшить интервал выгрузки.\n' \
                           f'Сейчас интервал равен {interval} дней.\n' \
                           f'Если вы видите эту ошибку, просьба проинформировать о ней dmitry.kochnev@megafon.ru\n' \
                           f'Либо другого ответственного за работоспособность скрипта сотрудника.'
                    general.send_mail(os.environ.get('YA_HOST'), os.environ.get('YA_FROM'), w_email, subject, text)
                    exit()

            # формируется словарь параметров
            params = {
                "fields": f'{",".join(str(e) for e in fields)}',  # преобразование list в string
                "source": type_report,
                "date1": (dt_end - timedelta(days=period.days % interval)).strftime('%Y-%m-%d'),
                "date2": dt_end.strftime('%Y-%m-%d')
            }
            # проверка возможности создания отчета
            # https://yandex.ru/dev/metrika/doc/api2/logs/queries/evaluate.html
            possibility = self.client.evaluate().get(params=params)
            if possibility['log_request_evaluation']['possible']:
                # если создание отчета возможно
                logger.info('The report can be created')

                # post запрос на создание отчета
                try:
                    order = self.client.create().post(params=params)
                except ValueError as err:
                    subject = 'MetrikaLogsAPI. Download failed'
                    text = f'Выгрузка отчета завершилась ошибкой.\n' \
                           f'{err}'
                    general.send_mail(os.environ.get('YA_HOST'), os.environ.get('YA_FROM'), w_email, subject, text)
                # получение id запроса
                request_id = order['log_request']['request_id']
                logger.info(f'Id of the created report {request_id}')
                # получение информации о статусе готовности отчета
                info = self.client.info(requestId=request_id).get()

                # в цикле с паузой в 6 секунд проверяется статус готовности отчета
                # цикл завершится, если будет получен ответ processed
                while info['log_request']['status'] != 'processed':
                    info = self.client.info(requestId=request_id).get()
                    # если получен ответ processing_failed сообщает об этом, завершение скрипта
                    if info['log_request']['status'] == 'processing_failed':
                        logger.error('Error: processing_failed')
                        subject = 'MetrikaLogsAPI. processing_failed.'
                        text = f'LogsAPI отдало ошибку processing_failed.\n' \
                               f'Отчет не может быть сформирован, запустите скрипт заново.\n' \
                               f'Если вы видите эту ошибку, просьба проинформировать о ней dmitry.kochnev@megafon.ru\n' \
                               f'Либо другого ответственного за работоспособность скрипта сотрудника.'
                        general.send_mail(os.environ.get('YA_HOST'), os.environ.get('YA_FROM'), w_email, subject, text)
                        self.del_report(request_id)
                        exit()
                    time.sleep(6)
                    logger.info(f"Preparation of the report. Status: {info['log_request']['status']}")

                logger.info('Report is ready. Unloading...')

                # скачивание первой части отчета
                report = self.client.download(requestId=request_id).get()
                # преобразование в pandas DataFrame
                report_df = pd.DataFrame.from_dict(report().to_dicts())
                logger.info(f"Report size {len(info['log_request']['parts'])} part")
                # проверка из скольки частей состоит отчет,
                # если всего одна часть - двигаемся дальше,
                # если частей несколько в цикле выкачиваем все части начиная со второй
                if len(info['log_request']['parts']) > 1:
                    for i in info['log_request']['parts']:
                        if i['part_number'] == 0:
                            pass
                        else:
                            stat_response = 'timeout'
                            while stat_response == 'timeout':
                                try:
                                    part = self.client.download(requestId=request_id,
                                                                partNumber=i['part_number']).get()
                                    # преобразование в pandas DataFrame
                                    part_df = pd.DataFrame.from_dict(part().to_dicts())
                                    # объединяем с первой частью
                                    report_df = pd.concat([report_df, part_df])
                                    logger.info(f"Part {i['part_number']} processed")
                                    stat_response = 'good'
                                except ChunkedEncodingError as e:
                                    logger.warning('Response from LogsAPI timeout.')
                                    stat_response = 'timeout'

                logger.info('Report success')
                df_all_parts = pd.concat([df_all_parts, report_df])

                # удаляем отчет из logs api
                self.del_report(request_id)

            empty_df = df_all_parts.iloc[0:0]

            df_all_parts.to_sql(table_name,
                                con=engine,
                                # schema='dbo',
                                if_exists='fail',
                                index=False,
                                chunksize=20,
                                method='multi')


            logger.info('Done!')


def create_params(date: list, source: list, fields: dict):
    """
    Generates a dictionary of parameters for a request to the logs API
    :param date: The date for which the data needs to be uploaded
    :param source: Report type (visits/hits)
    :param fields: List of parameters for unloading
    :return: dict
    """
    params = {
        "fields": f'{",".join(str(e) for e in fields)}',  # преобразование list в string
        "source": source,
        "date1": date,
        "date2": date
    }
    return params
