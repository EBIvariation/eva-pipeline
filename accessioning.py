import os.path

import luigi
import shellout

import evapro_adaptor

__author__ = 'Cristina Yenyxe Gonzalez Garcia'


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

    def requires(self):
        return []

    def run(self):
        # Get study prefix and its last accession
        info = evapro_adaptor.get_variant_accessioning_info(self.file)
        if not info:
            raise evapro_adaptor.EvaproError('Filename not found in EVAPRO')
        (study_id, study_prefix, last_accession) = info

        print 'Creating variant accession IDs for study ' + study_id + ' (starting in ' + last_accession + ')'

        # Simplest command-line
        command = '/home/cyenyxe/appl/opencga/opencga create-accessions -i {input} -p ess -s {prefix} -o {outdir}'
        kwargs = {'input': self.file,
                  'prefix': study_prefix,
                  'outdir': self.vcf_dir}

        # Fill optional arguments
        # Use last_accession read from EVAPRO
        if last_accession:
            command += ' -r {resume}'
            kwargs['resume'] = last_accession

        # Launch tool
        shellout.shellout_no_stdout(command, **kwargs)

    def output(self):
        path_parts = os.path.basename(self.file).split(os.extsep)
        compressed = path_parts[-1] == 'gz'
        if compressed:
            filename = os.extsep.join(path_parts[:-2])  # For those names that use . instead of _ as separator :(
            extension = path_parts[-2]
        else:
            filename = os.extsep.join(path_parts[:-1])  # For those names that use . instead of _ as separator :(
            extension = path_parts[-1]

        print 'Path to accessioned file = ' + luigi.LocalTarget(self.vcf_dir + filename + '_accessioned.' + extension).fn
        return luigi.LocalTarget(self.vcf_dir + filename + '_accessioned.' + extension)


class SaveLastAccession(luigi.Task):
    """

    """

    file = luigi.Parameter(description='Input VCF file to process and load')
    vcf_dir = luigi.Parameter(description='Folder for storage of EVA VCF files')

    def requires(self):
        return VariantsAccessioning(self.file, self.vcf_dir)

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

        print 'Last accession ID generated = ' + max_accession

        evapro_adaptor.save_last_accession(self.file, max_accession)

    def output(self):
        return self.input()


if __name__ == '__main__':
    luigi.run()
