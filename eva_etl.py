import luigi
import random

# import accessioning

__author__ = 'Cristina Yenyxe Gonzalez Garcia'


class VariantsLoading(luigi.Task):

    # TODO Possible FileParameter or PathParameter class?
    file = luigi.Parameter(description='Input VCF file to process and load')
    vcf_dir = luigi.Parameter(description='Folder for storage of EVA VCF files')
    json_dir = luigi.Parameter(description='Folder for storage of EVA JSON files')

    def requires(self):
        return VariantsTransformation(self.file, self.vcf_dir)

    def run(self):
        print "load variants"

    def output(self):
        new_path = '/tmp/new_file%d.txt' % random.randint(0, 999999999)
        luigi.LocalTarget(self.input().fn).copy(new_path, True)
        return luigi.LocalTarget(new_path)


class VariantsTransformation(luigi.Task):

    file = luigi.Parameter(description='Input VCF file to process and load')
    vcf_dir = luigi.Parameter(description='Folder for storage of EVA VCF files')

    def requires(self):
        return []

    def run(self):
        print "transform variants"

    def output(self):
        new_path = '/tmp/file%d.txt' % random.randint(0, 999999999)
        luigi.LocalTarget(self.file).copy(new_path, True)
        return luigi.LocalTarget(new_path)


if __name__ == '__main__':
    luigi.run()
