import os.path

import luigi
import shellout

import evapro_adaptor

__author__ = 'Cristina Yenyxe Gonzalez Garcia'


class LoadLastAccession(luigi.Task):
    """
    Given a study prefix, return the last accession generated for that study (stored in EVAPRO).
    """
    study_prefix = luigi.Parameter(description='Prefix identifying the study in variant accession IDs')
    last_accession = 'not_valid_accession'
    path = '/tmp/' + last_accession

    def requires(self):
        return []

    def run(self):
        conn = evapro_adaptor.connect()
        cursor = conn.cursor()

        cursor.execute('SELECT last_used_accession FROM project_var_accession where project_prefix=\'{prefix}\''
                       .format(prefix=self.study_prefix))

        self.last_accession = '0000000'
        rows = tuple(cursor)
        if rows and rows[0]:
            self.last_accession = rows[0][0]

        print "The last accession created in study '" + self.study_prefix + "' was " + self.last_accession

        evapro_adaptor.disconnect(conn)
        self.path = '/tmp/' + self.last_accession
        open(self.path, 'a').close()

    def output(self):
        return luigi.LocalTarget(self.path)


class VariantsAccessioning(luigi.Task):
    """
    Run the OpenCGA variant accessioning tool, whose options are (* are mandatory):

    * -i, --input
             File to annotation with accession IDs
      -o, --outdir
             Directory where the output file will be saved
      -p, --prefix
             Accession IDs prefix
      -r, --resume-from-accession
             Starting point to generate accessions (will not be included)
    * -s, --study-alias
             Unique ID for the study where the file is classified (used for prefixes)
    """

    # TODO Possibly implement a FileParameter or PathParameter class?
    file = luigi.Parameter(description='Input VCF file to process and load')
    vcf_dir = luigi.Parameter(description='Folder for storage of EVA VCF files')
    study_prefix = luigi.Parameter(description='Prefix identifying the study in variant accession IDs')
    # last_accession = luigi.Parameter(default=None)

    def requires(self):
        return LoadLastAccession(study_prefix=self.study_prefix)

    def run(self):
        # Simplest command-line
        command = '/home/cyenyxe/appl/opencga/opencga create-accessions -i {input} -p ess -s {prefix} -o {outdir}'
        kwargs = {'input': self.file,
                  'prefix': self.study_prefix,
                  'outdir': self.vcf_dir}

        # Fill optional arguments
        # Use last_accession read from EVAPRO
        if self.input().fn:
            command += ' -r {resume}'
            last_accession = os.path.basename(self.input().fn)
            kwargs['resume'] = last_accession

        # Launch tool
        shellout.shellout_no_stdout(command, **kwargs)

    def output(self):
        (filename, extension) = os.path.splitext(os.path.basename(self.file))
        print 'Path to accessioned file = ' + luigi.LocalTarget(self.vcf_dir + filename + '_accessioned' + extension).fn
        return luigi.LocalTarget(self.vcf_dir + filename + '_accessioned' + extension)

    def on_success(self):
        # Delete file that contains the previous last accession
        input_name = self.input().fn
        self.input().remove()
        print 'Temporary file containing last accession removed (' + input_name + ')'


class SaveLastAccession(luigi.Task):

    file = luigi.Parameter(description='Input VCF file to process and load')
    vcf_dir = luigi.Parameter(description='Folder for storage of EVA VCF files')
    study_prefix = luigi.Parameter(description='Prefix identifying the study in variant accession IDs')

    def requires(self):
        return VariantsAccessioning(self.file, self.vcf_dir, self.study_prefix)

    def run(self):
        # Get the last lines in self.input()
        last_lines_file = shellout.shellout('tail -n 10 < {input} > {output}', input=self.input().fn)

        # Retrieve the last accession by lexicographical order
        max_accession = '00000000'
        with last_lines_file.open('r') as last_file:
            last_text = last_file.read()

        for line in last_text.split('\n'):
            if line:
                for field in line.split('\t')[7].split(';'):
                    if field.startswith('ACC='):
                        curr_accession = field.split('=')[1]
                        for acc in curr_accession.split(','):
                            if acc[-7:] > max_accession:
                                max_accession = acc[-7:]
                                print max_accession

        # Store the new last accession into PostgreSQL
        conn = evapro_adaptor.connect()
        cursor = conn.cursor()
        cursor.execute('UPDATE project_var_accession SET last_used_accession=\'{accession}\' '
                       'where project_prefix=\'{prefix}\''
                       .format(accession=max_accession, prefix=self.study_prefix))
        conn.commit()
        evapro_adaptor.disconnect(conn)

        return []

    def output(self):
        return self.input()


if __name__ == '__main__':
    luigi.run()
