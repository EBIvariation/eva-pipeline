import os
import json
import luigi

from project_initialization import CreateStudy
from shellout import shellout_no_stdout, shellout
import configuration


class CreateVariantsFile(luigi.Task):
  """
  Creates a variants file metadata in OpenCGA catalog, and checks its ID for completion
  """
  
  path = luigi.Parameter()
  
  study_alias = luigi.Parameter()
  study_name = luigi.Parameter(default="")
  study_description = luigi.Parameter(default="")
  study_uri = luigi.Parameter(default="")
  study_ticket_uri = luigi.Parameter(default="")
  
  project_alias = luigi.Parameter()
  project_name = luigi.Parameter(default="")
  project_description = luigi.Parameter(default="")
  project_organization = luigi.Parameter(default="")
  
  
  def requires(self):
    return CreateStudy(alias=self.study_alias, name=self.study_name, description=self.study_description, 
                       uri=self.study_uri, ticket_uri=self.study_ticket_uri,
                       project_alias=self.project_alias, project_name=self.project_name, 
                       project_description=self.project_description, project_organization=self.project_organization)
  
  def run(self):
    config = configuration.get_opencga_config('pipeline_config.conf')
    command = '{opencga-root}/bin/opencga.sh files create --user {user} --password {password} ' \
              '-i "{path}" --study-id "{user}@{project-alias}/{study-alias}" --bioformat VARIANT --checksum --output-format IDS'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'path'            : self.path,
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias}
    shellout_no_stdout(command, **kwargs)
    
    
  def complete(self):
    config = configuration.get_opencga_config('pipeline_config.conf')
    command = '{opencga-root}/bin/opencga.sh files info --user {user} --password {password} ' \
              '-id "{user}@{project-alias}/{study-alias}/{filename}" --output-format IDS > {output}'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias,
              'filename'        : os.path.basename(self.path),
              'output'          : "/tmp/" + os.path.basename(self.path) + ".step1"}
    
    # If the file was found, the output file will have some contents
    try:
      output_path = shellout(command, **kwargs)
    except RuntimeError:
      return False
    return os.path.getsize(output_path) > 0
  


class TransformFile:
  """
  Transforms a VCF file to an intermediate data model JSON file
  """
  
  path = luigi.Parameter()
  
  study_alias = luigi.Parameter()
  study_name = luigi.Parameter(default="")
  study_description = luigi.Parameter(default="")
  study_uri = luigi.Parameter(default="")
  study_ticket_uri = luigi.Parameter(default="")
  
  project_alias = luigi.Parameter()
  project_name = luigi.Parameter(default="")
  project_description = luigi.Parameter(default="")
  project_organization = luigi.Parameter(default="")
  
  
  def requires(self):
    return CreateVariantsFile(self.path, self.study_alias, self.study_name, self.study_description, self.study_uri, self.study_ticket_uri,
                              self.project_alias, self.project_name, self.project_description, self.project_organization)
  
  def complete(self):
    """
    To check completion, the project ID and study must be retrieved, and the json.snappy files searched in the folder specified in OpenCGA Catalog
    """
    
    project_id = -1
    study_id = -1
    config = configuration.get_opencga_config('pipeline_config.conf')
    
    # Get project numerical ID
    command = '{opencga-root}/bin/opencga.sh projects info --user {user} --password {password} ' \
              '-id "{user}@{project-alias}" --output-format IDS > {output}'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'project-alias'   : self.project_alias,
              'output'          : "/tmp/" + os.path.basename(self.path) + ".project"}
    
    try:
      project_output = shellout(command, **kwargs)
    except RuntimeError:
      return False
    
    if os.path.getsize(project_output) > 0:
      with open(project_output, 'r') as file:
        project_id = int(file.read())
    
    # Get study numerical ID and data folder
    command = '{opencga-root}/bin/opencga.sh studies info --user {user} --password {password} ' \
              '-id "{user}@{project-alias}/{study-alias}" > {output}'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias,
              'output'          : "/tmp/" + os.path.basename(self.path) + ".study"}  
    
    try:
      study_output = shellout(command, **kwargs)
    except RuntimeError:
      return False
    
    if os.path.getsize(study_output) > 0:
      with open(study_output, 'r') as file:
        study_json = json.load(file)
        study_id = study_json['id']
        study_folder = study_json['uri'].split(':', 1)[1]  # Remove prefix "file:"
    
    files_root = "{folder}/{filename}".format(folder=study_folder, filename=os.path.basename(self.path))
    
    # The project and study ID must be at least zero, and the output files must exist
    return project_id > -1 and study_id > -1 and \
           os.path.isfile(files_root + '.file.json.snappy') and \
           os.path.isfile(files_root + '.variants.json.snappy')

  def output(self):
    """
    The output files are json.snappy tranformed from VCF, and searched in the folder specified in OpenCGA Catalog
    """
    
    project_id = -1
    study_id = -1
    config = configuration.get_opencga_config('pipeline_config.conf')
    
    # Get project numerical ID
    command = '{opencga-root}/bin/opencga.sh projects info --user {user} --password {password} ' \
              '-id "{user}@{project-alias}" --output-format IDS > {output}'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'project-alias'   : self.project_alias,
              'output'          : "/tmp/" + os.path.basename(self.path) + ".project"}
    
    project_output = shellout(command, **kwargs)
    if os.path.getsize(project_output) > 0:
      with open(project_output, 'r') as file:
        project_id = int(file.read())
    
    # Get study numerical ID and data folder
    command = '{opencga-root}/bin/opencga.sh studies info --user {user} --password {password} ' \
              '-id "{user}@{project-alias}/{study-alias}" > {output}'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias,
              'output'          : "/tmp/" + os.path.basename(self.path) + ".study"}  
    
    study_output = shellout(command, **kwargs)
    if os.path.getsize(study_output) > 0:
      with open(study_output, 'r') as file:
        study_json = json.load(file)
        study_id = study_json['id']
        study_folder = study_json['uri'].split(':', 1)[1]  # Remove prefix "file:"
    
    files_root = "{folder}/{filename}".format(folder=study_folder, filename=os.path.basename(self.path))
    
    return { 'variants' : luigi.LocalTarget(files_root + '.variants.json.snappy'),
             'file'     : luigi.LocalTarget(files_root + '.file.json.snappy') }



class TransformGenotypesFile(luigi.Task, TransformFile):
  
  def requires(self):
    return TransformFile.requires(self)
    
  def run(self):
    config = configuration.get_opencga_config('pipeline_config.conf')
    command = '{opencga-root}/bin/opencga.sh files index --user {user} --password {password} ' \
              '--file-id "{user}@{project-alias}/{study-alias}/{filename}" --output-format IDS ' \
              '-Dannotate=false -- --transform --include-genotypes'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias,
              'filename'        : os.path.basename(self.path)}
    shellout_no_stdout(command, **kwargs)
  
  def complete(self):
    return TransformFile.complete(self)
  
  def output(self):
    return TransformFile.output(self)
  
  

class TransformAggregatedFile(luigi.Task, TransformFile):
  
  aggregation = luigi.Parameter()
  mapping_file = luigi.Parameter(default=None)
  
  def requires(self):
    return TransformFile.requires(self)
    
  def run(self):
    config = configuration.get_opencga_config('pipeline_config.conf')
    command = '{opencga-root}/bin/opencga.sh files index --user {user} --password {password} ' \
              '--file-id "{user}@{project-alias}/{study-alias}/{filename}" --output-format IDS ' \
              '-Dannotate=false -- --transform --aggregated {aggregation} --include-stats'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias,
              'filename'        : os.path.basename(self.path),
              'aggregation'     : self.aggregation}
    
    if self.mapping_file:
       command += ' --aggregation-mapping-file {map-file}'
       kwargs['map-file'] = self.mapping_file
    
    shellout_no_stdout(command, **kwargs)

  def complete(self):
    return TransformFile.complete(self)
  
  def output(self):
    return TransformFile.output(self)
  


class LoadFile:
  """
  Load transformed files into Mongo
  """
  
  path = luigi.Parameter()
  database = luigi.Parameter()
  
  study_alias = luigi.Parameter()
  study_name = luigi.Parameter(default="")
  study_description = luigi.Parameter(default="")
  study_uri = luigi.Parameter(default="")
  study_ticket_uri = luigi.Parameter(default="")
  
  project_alias = luigi.Parameter()
  project_name = luigi.Parameter(default="")
  project_description = luigi.Parameter(default="")
  project_organization = luigi.Parameter(default="")

# def complete(self):
#     # TODO Checking whether the loading run properly must be implemented



class LoadGenotypesFile(luigi.Task, LoadFile):
  
  def requires(self):
    return TransformGenotypesFile(self.path, self.study_alias, self.study_name, self.study_description, self.study_uri, self.study_ticket_uri,
                         self.project_alias, self.project_name, self.project_description, self.project_organization)
  
  def run(self):
    config = configuration.get_opencga_config('pipeline_config.conf')
    command = '{opencga-root}/bin/opencga.sh files index --user {user} --password {password} ' \
              '--database {database} --file-id "{user}@{project-alias}/{study-alias}/{variants-file}" ' \
              '--indexed-file-id "{user}@{project-alias}/{study-alias}/{filename}.MONGODB" --output-format IDS ' \
              '-Dannotate=false -- --load --include-genotypes --compress-genotypes'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'database'        : self.database,
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias,
              'variants-file'   : os.path.basename(self.input()['variants'].fn),
              'filename'        : os.path.basename(self.path)}
    shellout_no_stdout(command, **kwargs)
    


class LoadAggregatedFile(luigi.Task, LoadFile):
  
  aggregation = luigi.Parameter(default=None)
  mapping_file = luigi.Parameter(default=None)
  
  def requires(self):
    return TransformAggregatedFile(self.path, self.study_alias, self.study_name, self.study_description, self.study_uri, self.study_ticket_uri,
                         self.project_alias, self.project_name, self.project_description, self.project_organization, self.aggregation, self.mapping_file)
  
  def run(self):
    config = configuration.get_opencga_config('pipeline_config.conf')
    command = '{opencga-root}/bin/opencga.sh files index --user {user} --password {password} ' \
              '--database {database} --file-id "{user}@{project-alias}/{study-alias}/{variants-file}" ' \
              '--indexed-file-id "{user}@{project-alias}/{study-alias}/{filename}.MONGODB" --output-format IDS ' \
              '-Dannotate=false -- --load --include-stats'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'database'        : self.database,
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias,
              'variants-file'   : os.path.basename(self.input()['variants'].fn),
              'filename'        : os.path.basename(self.path)}
    shellout_no_stdout(command, **kwargs)



if __name__ == '__main__':
    luigi.run()
