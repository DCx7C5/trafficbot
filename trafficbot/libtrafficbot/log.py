import logging
import coloredlogs
from selenium.webdriver.remote.remote_connection import LOGGER


LOGGER.setLevel(logging.WARNING)

logger = logging.getLogger('TRAFFICBOT')

u_log = logging.getLogger('urllib3')
u_log.setLevel(logging.CRITICAL)

s_log = logging.getLogger('socks')
s_log.setLevel(logging.WARNING)

e_log = logging.getLogger('easyprocess')
e_log.setLevel(logging.WARNING)

p_log = logging.getLogger('pyvirtualdisplay')
p_log.setLevel(logging.CRITICAL)

coloredlogs.install(
    level=logging.DEBUG,
    fmt=f'%(asctime)-20s- %(name)-31s - %(process)-6s- %(levelname)-7s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
