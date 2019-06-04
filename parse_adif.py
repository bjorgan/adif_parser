"""
Quick and dirty ADIF parser.

See parse_adif() for entry method for parsing a single log
file, and get_all_logs_in_parent() for traversing a root
directory and collecting all adif files in a single Pandas
dataframe.
"""

import re
import pandas as pd

def extract_adif_column(adif_file, column_name):
    """
    Extract data column from ADIF file (e.g. 'OPERATOR' column).

    Parameters
    ----------
    adif_file: file object
        ADIF file opened using open().
    column_name: str
        Name of column (e.g. OPERATOR).
    Returns
    -------
    matches: list of str
        List of values extracted from the ADIF file.
    """

    pattern = re.compile('^.*<' + column_name + ':\d+>([^<]*)<.*$', re.IGNORECASE)
    matches = [re.match(pattern, line)
            for line in adif_file]
    matches = [line[1].strip() for line in matches if line is not None]
    adif_file.seek(0)

    if len(matches) > 0:
        return matches
    else:
        return None

OPERATOR_COLUMN_NAME = 'OPERATOR'
DATE_COLUMN_NAME = 'QSO_DATE'
CALL_COLUMN_NAME = 'CALL'
TIME_COLUMN_NAME = 'TIME_ON'

def parse_adif(filename, extra_columns=[]):
    """
    Parse ADIF file into a pandas dataframe.  Currently tries to find operator,
    date, time and call fields. Additional fields can be specified.

    Parameters
    ----------
    filename: str
        Path to ADIF file.
    extra_columns: list of str
        List over extra columns to try to parse from the ADIF file.
    Returns
    -------
    df: Pandas DataFrame
        DataFrame containing parsed ADIF file contents.
    """

    df = pd.DataFrame()
    adif_file = open(filename, 'r', encoding="iso8859-1")

    try:
        df = pd.DataFrame({
                'operator': extract_adif_column(adif_file, OPERATOR_COLUMN_NAME),
                'date': extract_adif_column(adif_file, DATE_COLUMN_NAME),
                'time': extract_adif_column(adif_file, TIME_COLUMN_NAME),
                'call': extract_adif_column(adif_file, CALL_COLUMN_NAME),
                'filename': os.path.basename(filename)
                })

        for column in extra_columns:
            df[column] = extract_adif_column(adif_file, column)
    except:
        return None
    return df

import os

def get_all_logs_in_parent(root_path):
    """
    Walk the file tree beginning at input root path,
    parse all adif logs into a common dataframe.

    Parameters
    ----------
    root_path: str
        Root path.
    Returns
    -------
    qsos: Pandas DataFrame
        DataFrame containing all QSOs that could be parsed from ADIF files
        contained in root_path.
    """
    qsos = pd.DataFrame()

    for root, dirs, files in os.walk(root_path):
        for filename in files:
            if filename.endswith(('.adi', '.ADI')):
                path = os.path.join(root, filename)
                qsos = pd.concat((qsos, parse_adif(path)))
    return qsos
