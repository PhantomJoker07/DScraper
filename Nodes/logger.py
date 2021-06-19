import os
import logging
import threading

lock = threading.Lock()

logging.basicConfig(filename='debug.log',level=logging.DEBUG)
#logging.basicConfig(level = logging.DEBUG)

logger = logging.getLogger(__name__)

def debug_log(_id,message=''):
    lock.acquire()
    msg = str(_id) + ":: " + str(message)
    logging.debug(msg)
    lock.release()

def error_log(message):
    logging.error(message)

def clear_log():
    with open('debug.log', 'wb') as fd:
        fd.write(b'')
