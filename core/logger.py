import logging
import os
import sys
from datetime import datetime

file_dir = os.path.dirname(os.path.realpath('__file__'))

df = datetime.now().strftime("%Y%m%d-%H%M%S")
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S',
    level=logging.INFO,
    filename=file_dir + f'/log/{df}.log'
)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

log = logging.getLogger('__name__')
