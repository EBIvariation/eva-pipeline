import ConfigParser

__author__ = 'Cristina Yenyxe Gonzalez Garcia'


def get_evapro_config(filename):
    config = ConfigParser.SafeConfigParser()
    config.read(filename)

    host = config.get('evapro', 'host')
    port = config.getint('evapro', 'port')
    database = config.get('evapro', 'database')
    user = config.get('evapro', 'user')
    password = config.get('evapro', 'password')

    if not host or not port or not database or not user or not password:
        raise ConfigParser.ParsingError('Connection to EVAPRO database not properly configured, '
                                        'please check your {filename} file'.format(filename=filename))

    return {'host': host, 'port': port, 'database': database, 'user': user, 'password': password}


def get_opencga_config(filename):
    config = ConfigParser.SafeConfigParser()
    config.read(filename)

    root_folder = config.get('opencga', 'root_folder')

    if not root_folder:
        raise ConfigParser.ParsingError('References to OpenCGA installation not properly configured, '
                                        'please check your {filename} file'.format(filename=filename))

    return {'root_folder': root_folder}
