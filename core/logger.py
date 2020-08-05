import logging
import os
import sys


file_dir = os.path.dirname(os.path.realpath('__file__'))

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S',
    level=logging.DEBUG,
    filename=file_dir + '/log/logs.txt'
)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

log = logging.getLogger('__name__')
