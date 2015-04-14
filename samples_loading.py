import json
import logging
import os

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
    return CreateSamplesFile(self.path, self.study_alias, self.study_name, self.study_description, self.study_uri, self.study_ticket_uri,
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
              'study-alias'     : self.study_alias,
              'filename'        : os.path.basename(self.path)}
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
  Create a cohort named 'all' which groups all the samples from a study
  """
  
  #path = luigi.Parameter()
  
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
    # Look up for all the samples associated to the study
    command = '{opencga-root}/bin/opencga.sh samples search --user {user} --password {password} ' \
              '--study-id "{user}@{project-alias}/{study-alias}" --output-format ID_CSV > {output}'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias,
              'output'          : "/tmp/" + self.study_alias + ".samples"}
    
    output_path = shellout(command, **kwargs)
    with open(output_path, 'r') as file:
      samples = file.read()
    
    # Create a cohort using all the samples from the study
    command = '{opencga-root}/bin/opencga.sh cohorts create --user {user} --password {password} ' \
              '--study-id "{user}@{project-alias}/{study-alias}" --name all --sample-ids {samples}'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias,
              'samples'         : samples}
    
    shellout_no_stdout(command, **kwargs)
    
    
  def complete(self):
    """
    Check that all the study samples and those from the cohort are the same
    """
    
    config = configuration.get_opencga_config('pipeline_config.conf')
    # Look up for all the samples associated to the study
    command = '{opencga-root}/bin/opencga.sh samples search --user {user} --password {password} ' \
              '--study-id "{user}@{project-alias}/{study-alias}" --output-format ID_CSV > {output}'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias,
              'output'          : "/tmp/" + self.study_alias + ".step3_1"}
    
    try:
      output_path = shellout(command, **kwargs)
    except RuntimeError:
      return False
    
    with open(output_path, 'r') as file:
      all_samples = file.read().strip().split(',')
      
    try:
      all_samples = map(int, all_samples) # Store as integers
    except ValueError:
      # This means there are no samples to process
      print('Study %s has no samples loaded' % self.study_alias)
      return True
    
    # Retrieve the samples for the cohort named "all"
    command = '{opencga-root}/bin/opencga.sh studies info --user {user} --password {password} ' \
              '--study-id "{user}@{project-alias}/{study-alias}" > {output}'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias,
              'output'          : "/tmp/" + self.study_alias + ".step3_2"}
      
    try:
      output_path = shellout(command, **kwargs)
    except RuntimeError:
      return False
    
    cohort_samples = []
    if os.path.getsize(output_path) > 0:
      with open(output_path, 'r') as file:
        study_json = json.load(file)
        study_cohorts = study_json['cohorts']
      for cohort in study_cohorts:
        if cohort['name'] == 'all':
          cohort_samples = cohort['samples']
          break
    
    # Check the set of all the study samples and the cohort are equal
    return sorted(all_samples) == sorted(cohort_samples)



class CreateCohort(luigi.Task):
  pass
  


if __name__ == '__main__':
  luigi.run()
