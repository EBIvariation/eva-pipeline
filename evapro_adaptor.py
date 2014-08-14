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


def get_study_and_file_id(filename):
    """
    Given a filename, returns the submission ID of the project where it is classified and the file submission ID in ENA.

    :param filename: The name of the file to search
    :return: File and project submission ID
    """
    conn = connect()
    cursor = conn.cursor()

    cursor.execute('SELECT project.project_accession, file.ena_submission_file_id '
                   'FROM project, project_analysis, analysis_file, file '
                   'WHERE project.project_accession = project_analysis.project_accession '
                   'AND project_analysis.analysis_accession = analysis_file.analysis_accession '
                   'AND analysis_file.file_id = file.file_id AND file.filename = \'{fname}\''
                   .format(fname=filename))

    rows = tuple(cursor)
    info = rows[0] if rows and rows[0] else None
    disconnect(conn)
    return info


def get_variant_accessioning_info(filename):
    """
    Given a filename, returns the accession ID of the project where it is classified, its prefix for variant
    accessioning and the last variant accession generated.

    :param filename: The name of the file to search
    :return: Project submission ID, its prefix for accessioning and the last variant accession generated
    """
    conn = connect()
    cursor = conn.cursor()

    cursor.execute('SELECT project.project_accession, project_var_accession.project_prefix, '
                   'project_var_accession.last_used_accession '
                   'FROM project_var_accession, project, project_analysis, analysis_file, file '
                   'WHERE project_var_accession.project_accession_code = project.project_accession '
                   'AND project.project_accession = project_analysis.project_accession '
                   'AND project_analysis.analysis_accession = analysis_file.analysis_accession '
                   'AND analysis_file.file_id = file.file_id AND file.filename = \'{fname}\''
                   .format(fname=filename))

    rows = tuple(cursor)
    info = rows[0] if rows and rows[0] else None
    disconnect(conn)
    return info


def save_last_accession(filename, last_accession):
    conn = connect()
    cursor = conn.cursor()

    # Get study prefix
    info = get_variant_accessioning_info(filename)
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
