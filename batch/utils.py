""" Utility functions related to batch pipeline
"""

import logging
import json
import argparse


def setup_logging(filename, loglevel):
    """
    :param filename: relative path and filename of log
    """
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(lineno)s - %(funcName)20s() %(message)s"
    logging.basicConfig(
            filename=filename,
            format=log_format,
            level=loglevel,
            filemode = 'w'
            )


def parse_arguments():
    """ Parses command line arguments passed from shell script
    :returns: parser object
    """
    parser = argparse.ArgumentParser(description='Process data with spark-nlp')
    parser.add_argument(
            '-b', '--batchsize',
            action='store',
            type=int,
            default=2,
            help='number of rows to run through batch processing'
            )
    args = parser.parse_args()
    logging.info(json.dumps(vars(args)))
    return args
