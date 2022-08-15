import csv
import io
import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from os.path import basename, dirname, splitext

# bless up Martijn Pieters & Sriram Ragunathan of Stack Overflow
from Caladrius.auxiliary import mkdir


class PathTruncatingFormatter(logging.Formatter):
    def format(self, record):
        if 'pathname' in record.__dict__.keys():
            # truncate the pathname
            filename = basename(record.pathname)
            if filename == '__init__.py':
                filename = basename(dirname(record.pathname)) + '/__init__.py'
            record.pathname = filename
        return super(PathTruncatingFormatter, self).format(record)


def get_stdout_format(multithread=False):
    return '%(asctime)s.%(msecs)03d %(levelname)-8s{}[%(pathname)s:%(funcName)s:%(lineno)d] %(message)s' \
        .format('%(threadName)12s:%(thread)-6d' if multithread else ' ')


def get_csv_headers(multithread=False):
    if multithread:
        return ['Timestamp', 'Level', 'Thread_Name', 'Thread_ID', 'Source', 'Line_Number', 'Message']
    else:
        return ['Timestamp', 'Level', 'Source', 'Line_Number', 'Message']


def get_csv_format(multithread=False):
    return '%(asctime)s.%(msecs)03d,%(levelname)s,{}%(pathname)s:%(funcName)s,%(lineno)d,%(message)s' \
        .format('%(threadName)s,%(thread)d,' if multithread else '')


def get_logger(path_to_logger, name=None, multithread=False, stream_level=logging.DEBUG, file_level=logging.DEBUG,
               when='W0') -> logging.Logger:
    if not name:
        name = splitext(basename(path_to_logger))[0]

    logger = logging.getLogger(name)
    log_formatter = PathTruncatingFormatter(get_stdout_format(multithread))
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(stream_level)
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)
    file_handler = TimedRotatingFileHandler(path_to_logger, when=when)
    file_handler.setLevel(file_level)
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)

    logger.setLevel(logging.DEBUG)
    return logger


class CsvFormatter(logging.Formatter):
    def __init__(self, multithread=False):
        # super().__init__()
        super(CsvFormatter, self).__init__(fmt=get_csv_format(multithread), datefmt='%Y-%m-%dT%H:%M:%S')
        self.output = io.StringIO()
        self.writer = csv.writer(self.output, quoting=csv.QUOTE_ALL)

    def format(self, record):
        msg = record.getMessage()

        msg = msg.replace(',', '')
        msg = msg.replace('\n', ' ').replace('\r', '')
        record.msg = msg.strip()
        if 'pathname' in record.__dict__.keys():
            # truncate the pathname
            filename = basename(record.pathname)
            if filename == '__init__.py':
                filename = basename(dirname(record.pathname)) + '/__init__.py'

            record.pathname = filename

        s = super(CsvFormatter, self).format(record=record)
        if record.exc_text:
            s = s.replace('\n', '\\n')
        return s

    # def formatException(self, exc_info):
    #     result = super(CsvFormatter, self).formatException(exc_info)
    #     return repr(result)  # or format into one line however you want to


def get_csv_logger(path_to_logger, name=None, multithread=False,
                   stream_level=logging.DEBUG, file_level=logging.DEBUG,
                   file_handler: logging.FileHandler = None) -> logging.Logger:
    if not name:
        name = splitext(basename(path_to_logger))[0]
    logger = logging.getLogger(name)

    console_formatter = PathTruncatingFormatter(get_stdout_format(multithread))
    if stream_level is not None:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(stream_level)
        stream_handler.setFormatter(console_formatter)
        logger.addHandler(stream_handler)

    mkdir(dirname(path_to_logger))
    if not file_handler:
        file_handler = logging.FileHandler(path_to_logger)

    file_handler.setLevel(file_level)
    file_handler.setFormatter(CsvFormatter(multithread))
    logger.addHandler(file_handler)

    logger.setLevel(logging.DEBUG)
    return logger
