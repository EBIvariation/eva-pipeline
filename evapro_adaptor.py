import psycopg2

import configuration

__author__ = 'Cristina Yenyxe Gonzalez Garcia'


def connect():
    """
    Get a psycopg2 connection object to the database where the table is
    """
    config = configuration.get_evapro_config('pipeline_config.conf')
    connection = psycopg2.connect(host=config['host'], port=config['port'], database=config['database'],
                                  user=config['user'], password=config['password'])
    connection.set_client_encoding('utf-8')
    return connection


def disconnect(connection):
    connection.close()


def get_study_and_file_id(filename, eva_version):
    """
    Given a filename, returns the submission ID of the project where it is classified and the file submission ID in ENA.

    :param filename: The name of the file to search
    :return: File and project submission ID
    """
    conn = connect()
    cursor = conn.cursor()

    cursor.execute('SELECT project.project_accession, project.title, browsable_file.ena_submission_file_id '
                   'FROM project, project_analysis, analysis_file, browsable_file '
                   'WHERE project.project_accession = project_analysis.project_accession '
                   'AND project_analysis.analysis_accession = analysis_file.analysis_accession '
                   'AND analysis_file.file_id = browsable_file.file_id AND browsable_file.filename = \'{fname}\' '
                   'AND browsable_file.eva_release = \'{version}\' '
                   .format(fname=filename, version=eva_version))

    rows = tuple(cursor)
    info = rows[0] if rows and rows[0] else None
    disconnect(conn)
    return info


def get_variant_accessioning_info(filename, eva_version):
    """
    Given a filename, returns the accession ID of the project where it is classified, its prefix for variant
    accessioning and the last variant accession generated.

    :param filename: The name of the file to search
    :return: Project submission ID, its prefix for accessioning and the last variant accession generated
    """
    conn = connect()
    cursor = conn.cursor()

    cursor.execute('SELECT project.project_accession, pva.project_prefix, pva.last_used_accession '
                   'FROM project_var_accession as pva, project, project_analysis, analysis_file, browsable_file '
                   'WHERE pva.project_accession_code = project.project_accession_code '
                   'AND project.project_accession = project_analysis.project_accession '
                   'AND project_analysis.analysis_accession = analysis_file.analysis_accession '
                   'AND analysis_file.file_id = browsable_file.file_id AND browsable_file.filename = \'{fname}\' '
                   'AND browsable_file.eva_release = \'{version}\' '
                   .format(fname=filename, version=eva_version))

    rows = tuple(cursor)
    info = rows[0] if rows and rows[0] else None
    disconnect(conn)
    return info


def save_last_accession(filename, eva_version, last_accession):
    conn = connect()
    cursor = conn.cursor()

    # Get study prefix
    info = get_variant_accessioning_info(filename, eva_version)
    if not info:
        raise EvaproError('Filename not found in EVAPRO')
    study_prefix = info[1]

    # Store the new last accession into PostgreSQL
    cursor.execute('UPDATE project_var_accession SET last_used_accession=\'{accession}\' '
                   'where project_prefix=\'{prefix}\''
                   .format(accession=last_accession, prefix=study_prefix))

    conn.commit()
    disconnect(conn)


class EvaproError(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)

