import os


def getEnvPath(path):
    path = path.split('/')
    return '/'.join([os.getenv(f[1:]) if (f and f[0] == '$') else f for f in path])
