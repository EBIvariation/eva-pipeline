import errno
import os.path
import shutil

import luigi
from shellout import shellout_no_stdout

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
    last_accession = luigi.Parameter(default=None)
    study_prefix = luigi.Parameter()

    def run(self):
        # Simplest command-line
        command = '/home/cyenyxe/appl/opencga/opencga create-accessions -i {input} -p ess -s {prefix} -o /tmp/'
        kwargs = {'input': self.file,
                  'prefix': self.study_prefix}

        # Fill optional arguments
        if self.last_accession is not None and len(self.last_accession) > 0:
            command += ' -r {resume}'
            kwargs['resume'] = self.last_accession

        # Launch tool
        shellout_no_stdout(command, **kwargs)

        # Atomically move file
        try:
            luigi.File(path=self.temporary_output()).move(self.output().fn)
        except os.error, e:
            if e.errno == errno.EXDEV:
                # Fallback to copy
                shutil.copyfile(self.temporary_output(), self.output().fn)

    def temporary_output(self):
        print 'Temporary output = ' + luigi.LocalTarget('/tmp/' + os.path.basename(self.file) + '.out').fn
        return '/tmp/' + os.path.basename(self.file) + '.out'

    def output(self):
        print 'Final destination = ' + luigi.LocalTarget(self.vcf_dir + os.path.basename(self.file)).fn
        return luigi.LocalTarget(self.vcf_dir + os.path.basename(self.file))


if __name__ == '__main__':
    luigi.run()
