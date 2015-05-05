import os
import json
import luigi

from samples_loading import CreateGlobalCohort, CreatePopulationCohorts
from shellout import shellout_no_stdout, shellout
import configuration


class CreateVariantsStatistics(luigi.Task):
  
  path = luigi.Parameter()
  database = luigi.Parameter()
  #index_file_id = luigi.Parameter(default="")
  
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
    return [
             CreateGlobalCohort(self.study_alias, self.study_name, self.study_description, self.study_uri, self.study_ticket_uri,
                              self.project_alias, self.project_name, self.project_description, self.project_organization),
             CreatePopulationCohorts(self.study_alias, self.study_name, self.study_description, self.study_uri, self.study_ticket_uri,
                              self.project_alias, self.project_name, self.project_description, self.project_organization)
           ]

  def get_cohort_ids(self):
    config = configuration.get_opencga_config('pipeline_config.conf')

    command = '{opencga-root}/bin/opencga.sh studies info --user {user} --password {password} ' \
              '--study-id "{user}@{project-alias}/{study-alias}" > {output}'
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias,
              'output'          : "/tmp/" + self.study_alias + ".samples4_1"}

    try:
      output_path = shellout(command, **kwargs)
    except RuntimeError:
      return []

    if os.path.getsize(output_path) == 0:
      return []

    with open(output_path, 'r') as file:
      study_json = json.load(file)
      study_cohorts = study_json['cohorts']

    # Get the ids of all the cohorts created for the study
    cohorts_ids = [ str(cohort['id']) for cohort in study_cohorts ]
    return cohorts_ids


  def run(self):
    config = configuration.get_opencga_config('pipeline_config.conf')
    command = '{opencga-root}/bin/opencga.sh files stats-variants --user {user} --password {password} ' \
              '--cohort-id {cohort-id} --file-id "{user}@{project-alias}/{study-alias}/40_transformed/{filename}.MONGODB" ' \
              '--outdir-id "{user}@{project-alias}/{study-alias}/50_stats/" -- --create '
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias,
              'cohort-id'       : ",".join(self.get_cohort_ids()),
              'filename'        : os.path.basename(self.path)}
    
    shellout_no_stdout(command, **kwargs)
    

  def complete(self):
    return False


  def output(self):
    """
    The output files are json.gzip containing variants and files stats, and searched in the folder specified in OpenCGA Catalog
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
    
    #files_root = "{folder}/50_stats/{filename}".format(folder=study_folder, filename=os.path.basename(self.path))
    files_root = "{folder}/50_stats/stats_".format(folder=study_folder)
    
    # TODO Retrieve a list of all the cohorts
    # TODO Append list of underscore-separated cohort names
    # TODO Traverse 50_stats folder, looking for files that match that pattern (other list of cohorts mean it is not done)
    # TODO Get the newest files, whose date fragment will be the largest number
    
    return { 'variants' : luigi.LocalTarget(files_root + '.variants.stats.json.gz'),
             'file'     : luigi.LocalTarget(files_root + '.source.stats.json.gz') }
 


class LoadVariantsStatistics(luigi.Task):
  pass



if __name__ == '__main__':
    luigi.run()
