import psycopg2

__author__ = 'Cristina Yenyxe Gonzalez Garcia'


def connect():
    """
    Get a psycopg2 connection object to the database where the table is
    """
    connection = psycopg2.connect(host='ega2.ebi.ac.uk', port=5432, database='evapro',
                                  user='evapro', password='evapro')
    connection.set_client_encoding('utf-8')
    return connection


def disconnect(connection):
    connection.close()
