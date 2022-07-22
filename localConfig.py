import configparser
config = configparser.ConfigParser()

def getDict(section):
    data = {}
    for option in config.options(section):
        data[option] = config.get(section, option)
    return data


config.read('default.ini')
config.read('local.ini')
