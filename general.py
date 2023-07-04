import pandas as pd
import os
from loguru import logger
import datetime
import json
import bd_config
import argparse
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv

DATE_FORMAT = '%Y-%m-%d'
load_dotenv(bd_config.PATH_ENV)


def pretty_print() -> None:
    display = pd.options.display
    display.max_columns = 100
    display.max_rows = 100
    display.max_colwidth = 199
    display.width = None


def custom_log(proj):
    logger.add(bd_config.LOG_FILE,
               format=f'[{proj}] {{time}} {{level}} {{message}}',
               level='DEBUG',
               rotation='10000kb',
               compression='zip',
               serialize=True,
               enqueue=True)


def parse_log(log_file, max_date):
    data = []
    for line in open(log_file, 'r'):
        data.append(json.loads(line))
    project = []
    event_time = []
    event_timestamp = []
    file_name = []
    file_path = []
    lvl = []
    lvl_id = []
    module = []
    line = []
    message = []
    status_handler = []
    for i in range(len(data)):
        project.append(data[i].get('text').split(' ')[0].replace('[', '').replace(']', ''))
        event_time.append(datetime.datetime.strptime(data[i].get('record').get('time').get('repr')[0:-6],
                                                     '%Y-%m-%d %H:%M:%S.%f'))
        event_timestamp.append(str(data[i].get('record').get('time').get('timestamp')))
        file_name.append(data[i].get('record').get('file').get('name'))
        file_path.append(data[i].get('record').get('file').get('path'))
        lvl.append(data[i].get('record').get('level').get('name'))
        lvl_id.append(data[i].get('record').get('level').get('no'))
        module.append(data[i].get('record').get('module'))
        line.append(data[i].get('record').get('line'))
        message.append(data[i].get('record').get('message'))
        status_handler.append(0)
    col_names = ['project', 'event_time', 'event_timestamp', 'file_name', 'file_path', 'lvl', 'lvl_id', 'module',
                 'line', 'message', 'status_handler']
    log_table = pd.DataFrame(list(zip(project, event_time, event_timestamp, file_name, file_path, lvl, lvl_id, module,
                                      line, message, status_handler)), columns=col_names)
    log_table_filter = log_table.loc[log_table['event_time'] > max_date]
    return log_table_filter


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
        mailserver.set_debuglevel(True)
        mailserver.login(os.environ.get('YA_LOGIN'), os.environ.get('YA_PASS'))
        mailserver.sendmail(FROM, TO, msg.as_string())
        mailserver.quit()
        print("Письмо успешно отправлено")
    except smtplib.SMTPException:
        print("Ошибка: Невозможно отправить сообщение")


def daterange(start_date, end_date):
    "generates a list of dates "
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def employee_status(email, response_ad):
    status = {}
    if len(response_ad.get('entries')) != 0:
        status[email] = 'active'
    else:
        status[email] = 'not found'
    return status


def get_docx_from_dir(dir: str):
    """
    Retrieves the paths to docx files from the folder
    :param dir: The path to the folder to be scanned
    :return: list List of paths
    """
    paths = []
    for root, dirs, files in os.walk(dir):
        for file in files:
            if file.endswith('docx') and not file.startswith('~'):
                paths.append(os.path.join(root, file))
    return paths


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


def get_part_report(path: str):
    """
    Reads a json file and returns a dictionary
    :param path: The path to the config file
    :return: dict
    """
    with open(path) as input_file:
        part_file = json.loads(input_file.read())
    return part_file


def get_cli_options() -> argparse.Namespace:
    """
    Returns command line options
    :return: argparse.Namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-start_date', help='Start of period')
    parser.add_argument('-end_date', help='End of period')
    parser.add_argument('-mode', help='Mode (one of [history, reqular, regular_early])')
    parser.add_argument('-source', help='Source (hits or visits)')
    parser.add_argument('proj', type=str, help='Accepts the name of the integration project')
    options = parser.parse_args()
    validate_cli_options(options)
    return options


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


def get_source(options):
    """
    Parses the command line argument. Returns a list
    :param options: argparse.Namespace
    :return: list
    """
    if options.source == 'visits':
        source = ['visits']
    elif options.source == 'hits':
        source = ['hits']
    elif options.source == 'all':
        source = ['visits', 'hits']
    return source


def ym_params_replace(text: str, dic: dict) -> json:
    """
    Replaces values in a string, takes values from the dictionary
    :param text: The string in which you need to replace the values
    :param dic:Dictionary of values
               key - object replacement
               value - new value
    :return: JSON object
    """
    for i, j in dic.items():
        if text == '[]':
            return None
        text = text.replace(i, j)
    try:
        json_raw = json.loads(text)
    except json.decoder.JSONDecodeError:
        json_raw = {
            "status": "bad json"
        }
    return json_raw


def ym_parse_params_hitid(param: dict) -> str:
    if param is None:
        return None
    else:
        return param.get('hitID')


def ym_parse_params_h_content_store(param: dict) -> str:
    if param is None:
        return None
    else:
        return param.get('h_CONTENT_STORE')


def ym_parse_params_h_content_full_page(param: dict) -> str:
    if param is None:
        return None
    else:
        return param.get('h_CONTENT_FULL-PAGE')