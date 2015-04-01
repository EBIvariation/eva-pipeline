import os
import luigi

from shellout import shellout_no_stdout, shellout
import configuration
import evapro_adaptor


class CreateProject(luigi.Task):
  """
  Creates a project metadata in OpenCGA catalog, and checks its ID for completion
  """
  
  alias = luigi.Parameter()
  name = luigi.Parameter(default="")
  description = luigi.Parameter(default="")
  organization = luigi.Parameter(default="")
  

  def run(self):
    # If only an alias is provided, query the rest of arguments from EVAPRO
    if not self.name or not self.description or not self.organization:
      info = evapro_adaptor.get_study_info(self.alias)
      if not info:
          raise evapro_adaptor.EvaproError('Project not found in EVAPRO')
      (self.name, self.description, self.organization) = info
    
    # Create the project in OpenCGA Catalog
    config = configuration.get_opencga_config('pipeline_config.conf')
    command = '{opencga-root}/bin/opencga.sh projects create --user {user} --password {password} ' \
              '-n "{name}" -o "{organization}" -d "{description}" -a "{alias}" --output-format IDS'
    kwargs = {'opencga-root'	: config['root_folder'],
              'user'		: config['catalog_user'],
              'password'	: config['catalog_pass'],
              'name'		: self.name,
              'organization'	: self.organization,
              'description'	: self.description,
              'alias'		: self.alias}
    shellout_no_stdout(command, **kwargs)
    
    
  def complete(self):
    config = configuration.get_opencga_config('pipeline_config.conf')
    command = '{opencga-root}/bin/opencga.sh projects info --user {user} --password {password} ' \
              '--project-id "{user}@{alias}" --output-format IDS > {output}'
    kwargs = {'opencga-root'	: config['root_folder'],
              'user'		: config['catalog_user'],
              'password'	: config['catalog_pass'],
              'alias'		: self.alias,
              'output'		: self.alias}
    
    # If the project was found, the output file will have some contents
    try:
      output_path = shellout(command, **kwargs)
    except RuntimeError:
      return False
    return os.path.getsize(output_path) > 0

  
  
class CreateStudy(luigi.Task):
  """
  Creates a study metadata in OpenCGA catalog, and checks its ID for completion
  """
  
  alias = luigi.Parameter()
  name = luigi.Parameter(default="")
  description = luigi.Parameter(default="")
  uri = luigi.Parameter(default="")
  
  project_alias = luigi.Parameter()
  project_name = luigi.Parameter(default="")
  project_description = luigi.Parameter(default="")
  project_organization = luigi.Parameter(default="")
  
  
  def requires(self):
    return CreateProject(self.project_alias, self.project_name, self.project_description, self.project_organization)
  
  
  def run(self):
    # If only an alias is provided, query the rest of arguments from EVAPRO
    if not self.name or not self.description:
      info = evapro_adaptor.get_study_info(self.alias)
      if not info:
          raise evapro_adaptor.EvaproError('Study not found in EVAPRO')
      (self.name, self.description, organization) = info
      
    # Create the study in OpenCGA Catalog
    config = configuration.get_opencga_config('pipeline_config.conf')
    command = '{opencga-root}/bin/opencga.sh studies create --user {user} --password {password} ' \
              '--name "{name}" -d "{description}" -a "{alias}" --type CONTROL_SET ' \
              '--project-id "{user}@{project-alias}" --output-format IDS'
    kwargs = {'opencga-root'	: config['root_folder'],
              'user'		: config['catalog_user'],
              'password'	: config['catalog_pass'],
              'name'		: self.name,
              'description'	: self.description,
              'alias'		: self.alias,
              'project-alias'	: self.project_alias}
    
    if self.uri:
      command += ' --uri "file:{uri}"'
      kwargs['uri'] = self.uri
      
    shellout_no_stdout(command, **kwargs)
  
  
  def complete(self):
    config = configuration.get_opencga_config('pipeline_config.conf')
    command = '{opencga-root}/bin/opencga.sh studies info --user {user} --password {password} ' \
              '--study-id "{user}@{project-alias}/{alias}" --output-format IDS > {output}'
    kwargs = {'opencga-root'	: config['root_folder'],
              'user'		: config['catalog_user'],
              'password'	: config['catalog_pass'],
              'project-alias'	: self.project_alias,
              'alias'		: self.alias,
              'output'		: self.alias}
    
    # If the study was found, the output file will have some contents
    try:
      output_path = shellout(command, **kwargs)
    except RuntimeError:
      return False
    return os.path.getsize(output_path) > 0

  

if __name__ == '__main__':
    luigi.run()
