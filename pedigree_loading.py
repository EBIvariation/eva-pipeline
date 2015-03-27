import os
import luigi

from project_initialization import CreateStudy
from shellout import shellout_no_stdout, shellout
import configuration


class CreatePedigreeFile(luigi.Task):
  """
  Creates a pedigree file metadata in OpenCGA catalog, and checks its ID for completion
  """
  
  path = luigi.Parameter()
  
  study_alias = luigi.Parameter()
  study_name = luigi.Parameter(default="")
  study_description = luigi.Parameter(default="")
  
  project_alias = luigi.Parameter()
  project_name = luigi.Parameter(default="")
  project_description = luigi.Parameter(default="")
  project_organization = luigi.Parameter(default="")
  
  
  def requires(self):
    return CreateStudy(alias=self.study_alias, name=self.study_name, description=self.study_description, 
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
              'output'          : os.path.basename(self.path) + ".step1"}
    
    # If the file was found, the output file will have some contents
    try:
      output_path = shellout(command, **kwargs)
    except RuntimeError:
      return False
    return os.path.getsize(output_path) > 0



class LoadSamples(luigi.Task):
  """
  Loads samples metadata from a pedigree file
  """
  
  path = luigi.Parameter()
  
  study_alias = luigi.Parameter()
  study_name = luigi.Parameter(default="")
  study_description = luigi.Parameter(default="")
  
  project_alias = luigi.Parameter()
  project_name = luigi.Parameter(default="")
  project_description = luigi.Parameter(default="")
  project_organization = luigi.Parameter(default="")
  
  
  def requires(self):
    return CreatePedigreeFile(path=self.path, study_alias=self.study_alias, study_name=self.study_name, study_description=self.study_description, 
                              project_alias=self.project_alias, project_name=self.project_name, 
                              project_description=self.project_description, project_organization=self.project_organization)
  
  
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
    command = '{opencga-root}/bin/opencga.sh samples search --user {user} --password {password} ' \
              '--study-id "{user}@{project-alias}/{study-alias}" > {output}'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias,
              'alias'           : self.project_alias,
              'filename'        : os.path.basename(self.path),
              'output'          : os.path.basename(self.path) + ".step2"}
    
    # If the samples were found, the output file will have some contents
    try:
      output_path = shellout(command, **kwargs)
    except RuntimeError:
      return False
    
    # This tool writes a breakline at the end of the file
    return os.path.getsize(output_path) > 1
  
  

if __name__ == '__main__':
    luigi.run()
