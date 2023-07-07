import pandas as pd
import os
from loguru import logger
import datetime
import json
import config_env
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv

DATE_FORMAT = '%Y-%m-%d'
load_dotenv(config_env.PATH_ENV)


def pretty_print() -> None:
    display = pd.options.display
    display.max_columns = 100
    display.max_rows = 100
    display.max_colwidth = 199
    display.width = None



'''def send_mail(HOST, FROM, TO, SUBJECT, text):
    BODY = "\r\n".join((
        "From: %s" % FROM,
        "To: %s" % TO,
        "Subject: %s" % SUBJECT,
        "",
        text
    ))

    server = smtplib.SMTP(HOST)
    server.sendmail(FROM, [TO], BODY.encode('utf-8'))
    server.quit()'''


def send_mail(HOST: str, FROM: str, TO: str, SUBJECT: str, TEXT: str):
    msg = MIMEMultipart()
    msg['From'] = FROM
    msg['To'] = TO
    msg['Subject'] = SUBJECT
    message = TEXT
    msg.attach(MIMEText(message))
    try:
        mailserver = smtplib.SMTP_SSL(HOST, 465)
        # mailserver.set_debuglevel(True) # for debug
        mailserver.login(os.environ.get('YA_LOGIN'), os.environ.get('YA_PASS'))
        mailserver.sendmail(FROM, TO, msg.as_string())
        mailserver.quit()
        logger.info("Письмо успешно отправлено")
    except smtplib.SMTPException:
        logger.info("Ошибка: Невозможно отправить сообщение")


def get_config(path: str):
    """
    Reads the JSON file and puts it in the 'config' variable.
    Checks the presence of filled fields in JSON:
    - counter_id
    - token
    - retries
    - retries_delay
    If there is no data in the fields, it terminates the script execution with an error.
    :param path: The path to the config file
    :return: JSON
    """
    with open(path) as input_file:
        config = json.loads(input_file.read())

    assert 'counter_id' in config, 'CounterID must be specified in config'
    assert 'token' in config, 'Token must be specified in config'
    assert 'retries' in config, 'Number of retries should be specified in config'
    assert 'retries_delay' in config, 'Delay between retries should be specified in config'
    return config


def validate_cli_options(options) -> None:
    """
    Validates command line options
    :param options: argparse.Namespace
    :return: None
    """
    assert options.source is not None, \
        'Source must be specified in CLI options'
    if options.mode is None:
        assert (options.start_date is not None) \
               and (options.end_date is not None), \
            'Dates or mode must be specified'
    else:
        assert options.mode in ['history', 'regular', 'regular_early', 'auto'], \
            'Wrong mode in CLI options'


def get_date_period(options):
    """
    Generates a list of dates from the command line argument.
    :param options: argparse.Namespace
    :return: list
    """
    if options.mode is None:
        start_date = datetime.datetime.strptime(options.start_date, DATE_FORMAT)
        end_date = datetime.datetime.strptime(options.end_date, DATE_FORMAT)
        date_list = create_date_list(start_date, end_date)
    else:
        if options.mode == 'regular':
            start_date = (datetime.datetime.today() - datetime.timedelta(2))
            end_date = (datetime.datetime.today() - datetime.timedelta(2))
            date_list = create_date_list(start_date, end_date)
        elif options.mode == 'regular_early':
            start_date = (datetime.datetime.today() - datetime.timedelta(1))
            end_date = (datetime.datetime.today() - datetime.timedelta(1))
            date_list = create_date_list(start_date, end_date)
    return date_list


def create_date_list(start_date, end_date):
    date_list = []
    delta = end_date - start_date  # timedelta
    if delta.days < 0:
        logger.error(f'Дата окончания больше чем дата старта start_date = {start_date}, end_date = {end_date}')
        exit()
    for i in range(delta.days + 1):
        date_list.append((start_date + datetime.timedelta(i)).strftime(DATE_FORMAT))
    return date_list

