import os
import json
import luigi

from project_initialization import CreateStudy
from shellout import shellout_no_stdout, shellout
import configuration


class CreateSamplesFile(luigi.Task):
  """
  Creates a pedigree file metadata in OpenCGA catalog, and checks its ID for completion
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
              '-i "{path}" --study-id "{user}@{project-alias}/{study-alias}" --bioformat PEDIGREE --checksum --output-format IDS'
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
              'alias'           : self.project_alias,
              'filename'        : os.path.basename(self.path),
              'output'          : "/tmp/" + os.path.basename(self.path) + ".step1"}
    
    # If the file was found, the output file will have some contents
    try:
      output_path = shellout(command, **kwargs)
    except RuntimeError:
      return False
    return os.path.getsize(output_path) > 0



class LoadSamplesFile(luigi.Task):
  """
  Loads samples metadata from a pedigree file
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
    return CreatePedigreeFile(self.path, self.study_alias, self.study_name, self.study_description, self.study_uri, self.study_ticket_uri,
                              self.project_alias, self.project_name, self.project_description, self.project_organization)
  
  
  def run(self):
    config = configuration.get_opencga_config('pipeline_config.conf')
    command = '{opencga-root}/bin/opencga.sh samples load --user {user} --password {password} ' \
              '--pedigree-id "{user}@{project-alias}/{study-alias}/{filename}" --study-id "{user}@{project-alias}/{study-alias}" --output-format IDS'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'path'            : self.path,
              'project-alias'   : self.project_alias,
              'filename'        : os.path.basename(self.path),
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
              'alias'           : self.project_alias,
              'filename'        : os.path.basename(self.path),
              'output'          : "/tmp/" + os.path.basename(self.path) + ".step2_1"}
    
    # If the file was found, the output file will have some contents
    try:
      output_path = shellout(command, **kwargs)
    except RuntimeError:
      return False
    if os.path.getsize(output_path) == 0:
      return False
  
    # TODO If the samples were found, they should be a subset of those in the study
    command = '{opencga-root}/bin/opencga.sh samples search --user {user} --password {password} ' \
              '--study-id "{user}@{project-alias}/{study-alias}" > {output}'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias,
              'alias'           : self.project_alias,
              'filename'        : os.path.basename(self.path),
              'output'          : "/tmp/" + os.path.basename(self.path) + ".step2_2"}
    
    # If the samples were found, the output file will have some contents
    try:
      study_output = shellout(command, **kwargs)
    except RuntimeError:
      return False
    
    if os.path.getsize(study_output) == 0:
      return False
    
    # TODO The samples from the file and the database can't be compared until the JSON output from the CLI is valid
    #with open(study_output, 'r') as file:
      #study_json = json.load(file)
      #print(study_json)
    
      #samples_from_db = [ line.strip() for line in open(output_path, 'r') ]
      #samples_from_file = [ line.split(None, 1)[0] for line in open(self.path, 'r') ]
    
      #print(samples_from_db)
      #print(samples_from_file)
      
      #return samples_from_file.issubset(samples_from_db)
      
    return True
  
  
class CreateGlobalCohort(luigi.Task):
  """
  ALL_SAMPLES=$(opencga.sh samples search --study-id <study_id> --output-format ID_CSV)
  opencga.sh cohorts create --study-id <study_id> --sample-ids $ALL_SAMPLES
  """
  
  path = luigi.Parameter()
  
  study_alias = luigi.Parameter()
  study_name = luigi.Parameter(default="")
  study_description = luigi.Parameter(default="")
  study_uri = luigi.Parameter(default="")
  
  project_alias = luigi.Parameter()
  project_name = luigi.Parameter(default="")
  project_description = luigi.Parameter(default="")
  project_organization = luigi.Parameter(default="")
  
  
  def requires(self):
    return LoadSamplesFromPedigreeFile(self.path, self.study_alias, self.study_name, self.study_description, self.study_uri,
                                       self.project_alias, self.project_name, self.project_description, self.project_organization)
  
  def run(self):
    config = configuration.get_opencga_config('pipeline_config.conf')
    #command = '{opencga-root}/bin/opencga.sh files info --user {user} --password {password} ' \
              #'-id "{user}@{project-alias}/{study-alias}/{filename}" --output-format IDS > {output}'
    #kwargs = {'opencga-root'    : config['root_folder'],
              #'user'            : config['catalog_user'],
              #'password'        : config['catalog_pass'],
              #'project-alias'   : self.project_alias,
              #'study-alias'     : self.study_alias,
              #'alias'           : self.project_alias,
              #'filename'        : os.path.basename(self.path),
              #'output'          : "/tmp/" + os.path.basename(self.path) + ".step1"}
    
    
  def complete(self):
    return False


class CreateCohort(luigi.Task):
  pass
  

if __name__ == '__main__':
    luigi.run()
