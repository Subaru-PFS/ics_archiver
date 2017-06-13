import os
import logging
from twisted.python import log

def getEnvPath(path):
    path = path.split('/')
    return '/'.join([os.getenv(f[1:]) if (f and f[0] == '$') else f for f in path])

class LevelFileLogObserver(log.FileLogObserver):

    def __init__(self, f, level=logging.INFO):
        log.FileLogObserver.__init__(self, f)
        self.logLevel = level

    def emit(self, eventDict):
        if eventDict['isError']:
            level = logging.ERROR
        elif 'level' in eventDict:
            level = eventDict['level']
        else:
            level = logging.INFO
        if level >= self.logLevel:
            log.FileLogObserver.emit(self, eventDict)