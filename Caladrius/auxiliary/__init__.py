import hashlib
from abc import abstractmethod, ABC
from collections import defaultdict
from datetime import datetime, timedelta
from os.path import isfile, abspath, splitext
from pathlib import Path

import pandas as pd
import pytz


def mkdir(path_to_dir):
    if isinstance(path_to_dir, str):
        path_to_dir = Path(path_to_dir)
    elif isinstance(path_to_dir, Path):
        pass
    else:
        raise TypeError('Path to directory must be str or pathlib.Path')

    path_to_dir.mkdir(exist_ok=True, parents=True)
    return path_to_dir


def progress_bar(iterable, prefix='', suffix='', decimals=1, length=100, fill='█', print_end="\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iterable    - Required  : iterable object (Iterable)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        print_end    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    total = len(iterable)

    # Progress Bar Printing Function
    def print_progress_bar(iteration):
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=print_end)

    # Initial Call
    print_progress_bar(0)
    # Update Progress Bar

    if isinstance(iterable, pd.DataFrame):
        counter = 0
        for idx, row in iterable.iterrows():
            yield idx, row
            print_progress_bar(counter + 1)
            counter += 1
    else:
        for i, item in enumerate(iterable):
            yield i, item
            print_progress_bar(i + 1)
    # Print New Line on Complete
    print()


def noComma(word: str):
    return word.replace(',', '')


def xstr(s):
    return '' if s is None else str(s)


def xdt(dt: datetime, fmt: str, default: str = ''):
    if not dt:
        return default
    else:
        return dt.strftime(fmt)


def get_month_date(when: datetime = datetime.now(pytz.utc)):
    now_date = when.date()
    return now_date.strftime('%Y-%m'), now_date.strftime('%Y-%m-%d')


# You are given an array of n dicts,
# where it's guaranteed that each has keys [k1, k2, ... km]
# that together uniquely describe some set of values v1, v2, ... vm.
# Parse the array of n dicts to form a nested dictionary
# traversable by keys v1 ... vm
def reshape_dict_list(dict_list: list, keys: list):
    reshaped_dict = {}
    for a_dict in dict_list:
        new_keys = []
        for key in keys:
            new_keys.append(a_dict[key])
            del a_dict[key]

        value = reshaped_dict
        for i, new_key in enumerate(new_keys):
            is_leaf = i == len(new_keys) - 1
            if new_key not in value.keys():
                if is_leaf:
                    value[new_key] = a_dict
                else:
                    value[new_key] = {}
            value = value[new_key]

    return reshaped_dict


def get_available_filename(raw_filepath: str, limit=5):
    for i in range(limit):
        filename, ext = splitext(raw_filepath)
        new_filepath = f'{filename}_{i}{ext}' if i else raw_filepath
        if not isfile(new_filepath):
            return new_filepath
    raise ValueError(f'Could not find unique filepath for {abspath(raw_filepath)}')


class MyCache(ABC):
    def __init__(self, expiration_limit: timedelta):
        self.expiration_limit = expiration_limit
        self.cache = None
        self.exp = None

    @abstractmethod
    def refresh(self):
        pass

    def get(self, *args, **kwargs):
        if self.exp is None or datetime.utcnow() >= self.exp:
            self.cache = self.refresh()
            self.exp = datetime.utcnow() + self.expiration_limit
        return self.cache


def recursive_defaultdict():
    return defaultdict(recursive_defaultdict)


def solidify_recursive_defaultdict(d: defaultdict):
    for k, v in d.items():
        d[k] = solidify_recursive_defaultdict(v)

    return dict(d)


# Thanks to Programiz https://www.programiz.com/python-programming/examples/hash-file
def hash_file(filename):
    # make a hash object
    h = hashlib.sha1()

    # open file for reading in binary mode
    with open(filename, 'rb') as file:
        # loop till the end of the file
        chunk = 0
        while chunk != b'':
            # read only 1024 bytes at a time
            chunk = file.read(1024)
            h.update(chunk)

    # return the hex representation of digest
    return h.hexdigest()
