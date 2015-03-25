import os
import luigi

from shellout import shellout_no_stdout, shellout
import configuration


class CreateVariantsFile(luigi.Task):
  """
  Creates a file metadata in OpenCGA catalog, and checks its ID for completion
  """
  
  path = luigi.Parameter()
  
  study_alias = luigi.Parameter()
  study_name = luigi.Parameter(default="")
  study_description = luigi.Parameter(default="")
  
  project_alias = luigi.Parameter()
  project_name = luigi.Parameter(default="")
  project_description = luigi.Parameter(default="")
  project_organization = luigi.Parameter(default="")
  
  
  def depends(self):
    return CreateStudy(alias=study_alias, name=study_name, description=study_description, 
                       project_alias=project_alias, project_name=project_name, 
                       project_description=project_description, project_organization=project_organization)
  
  def run(self):
    config = configuration.get_opencga_config('pipeline_config.conf')
    command = '{opencga-root}/bin/opencga.sh files create --user {user} --password {password} ' \
              '-i "{path}" --study-id "{user}@{project-alias}/{study-alias}" --bioformat VARIANT --output-format IDS'
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
              '-id "{user}@{project-alias}/{study-alias}/{alias}" --output-format IDS > {output}'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias,
              'alias'           : self.project_alias,
              'output'          : self.project_alias}
    
    # If the file was found, the output file will have some contents
    output_path = shellout(command, **kwargs)
    return os.path.getsize(output_path) > 0
  
  
  
class TransformFile(luigi.Task):
  pass



class LoadFile(luigi.Task):
  pass



if __name__ == '__main__':
    luigi.run()
