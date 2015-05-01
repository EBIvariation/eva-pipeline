import os
import json
import luigi

from variants_loading import LoadGenotypesFile
from samples_loading import CreatePopulationCohorts
from shellout import shellout_no_stdout, shellout
import configuration


class CreateVariantsStatistics(luigi.Task):
  
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
  
  index_file_id = luigi.Parameter(default="")
  
  def requires(self):
    return [
             #LoadGenotypesFile(self.path, self.database, self.study_alias, self.study_name, self.study_description, self.study_uri, self.study_ticket_uri,
             #                  self.project_alias, self.project_name, self.project_description, self.project_organization),
             #CreatePopulationCohorts(self.study_alias, self.study_name, self.study_description, self.study_uri, self.study_ticket_uri,
             #                  self.project_alias, self.project_name, self.project_description, self.project_organization)
           ]

  def get_cohort_ids(self):

    config = configuration.get_opencga_config('pipeline_config.conf')

    # TODO Retrieve the number of possible values for variable 'Population'
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
    cohorts_ids = []
    for cohort in study_cohorts:
      cohorts_ids.append(cohort['id'])
    
    return cohorts_ids


  def run(self):
    config = configuration.get_opencga_config('pipeline_config.conf')
    # 
    command = '{opencga-root}/bin/opencga.sh files stats-variants --user {user} --password {password} ' \
              '--cohort-id {cohort_id} --file-id {index_file_id} ' \
              '--outdir-id "{user}@{project-alias}/{study-alias}/50_stats/" -- --create '
    kwargs = {'opencga-root'    : config['root_folder'],
              'user'            : config['catalog_user'],
              'password'        : config['catalog_pass'],
              'project-alias'   : self.project_alias,
              'study-alias'     : self.study_alias,
              'cohort_id'       : str(self.get_cohort_ids()).replace(" ", "")[1:-1],
              'index_file_id'   : self.index_file_id}
    
    shellout_no_stdout(command, **kwargs)
    
  
  def complete(self):
    return False
 


class LoadVariantsStatistics(luigi.Task):
  pass



if __name__ == '__main__':
    luigi.run()
