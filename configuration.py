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

    return { 'host': host, 'port': port, 'database': database, 'user': user, 'password': password }


def get_opencga_config(filename):
    config = ConfigParser.SafeConfigParser()
    config.read(filename)

    root_folder  = config.get('opencga', 'root_folder')
    catalog_host = config.get('opencga', 'catalog_host')
    catalog_port = config.getint('opencga', 'catalog_port')
    catalog_user = config.get('opencga', 'catalog_user')
    catalog_pass = config.get('opencga', 'catalog_pass')

    if not root_folder:
        raise ConfigParser.ParsingError('References to OpenCGA installation not properly configured, '
                                        'please check your {filename} file'.format(filename=filename))

    if not catalog_host or not catalog_port or not catalog_user or not catalog_pass:
        raise ConfigParser.ParsingError('References to OpenCGA catalog not properly configured, '
                                        'please check your {filename} file'.format(filename=filename))

    return { 
             'root_folder' : root_folder,
             'catalog_host' : catalog_host, 
             'catalog_port' : catalog_port,
             'catalog_user' : catalog_user,
             'catalog_pass' : catalog_pass
           }

