import configparser
import os.path as path

config = configparser.ConfigParser()

def getDict(section):
    data = {}
    for option in config.options(section):
        data[option] = config.get(section, option)
    return data


projectRoot = path.dirname(path.dirname(path.abspath(__file__)))

config.read(path.join(projectRoot, 'default.ini'))
config.read(path.join(projectRoot, 'local.ini'))
