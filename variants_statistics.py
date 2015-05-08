import os
import json
import luigi
import re

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
    
    print "study folder: " + study_folder
    # TODO Retrieve a list of all the cohorts
    
    cohort_names = []
    cohort_ids = self.get_cohort_ids()
    for cohort_id in cohort_ids:
      # Get cohort names from ids
      command = '{opencga-root}/bin/opencga.sh cohorts info --user {user} --password {password} ' \
                '-id "{cohort}" > {output}'
      kwargs = {'opencga-root'    : config['root_folder'],
                'user'            : config['catalog_user'],
                'password'        : config['catalog_pass'],
                'cohort'       : cohort_id,
                'output'          : "/tmp/" + os.path.basename(self.path) + ".study"}  
      
      cohort_output = shellout(command, **kwargs)
      if os.path.getsize(cohort_output) > 0:
        with open(cohort_output, 'r') as file:
          cohort_names.append(json.load(file)['name'])
      
    cohorts_string = "_".join(cohort_names)
    
    # TODO Append list of underscore-separated cohort names
    #files_root.append(cohorts_string)
    files_re = "stats_{cohorts}\.".format(cohorts=cohorts_string) # has to include the "." because of the case where cohorts="ABC" and there is another like "stats_ABC_DEF.source..."
        
    # TODO Traverse 50_stats folder, looking for files that match that pattern (other list of cohorts mean it is not done)
    stats_files = [ file for file in os.listdir("{folder}/50_stats/".format(folder=study_folder)) if re.match(files_re, file)]
    
    print "stats_files : {files}".format(files=stats_files)
    # TODO Get the newest files, whose date fragment will be the largest number
    files_prefix = "stats_{cohorts}.".format(cohorts=cohorts_string)
    clipped = [ file.replace(files_prefix, "") for file in stats_files ]    # make the filename start with the date
        
    dates = []
    dates_and_types = []
    stats_map = {}
    for name in clipped:    # get this: {'20150506152026': [], '20150506151837': [], ... }
      split = name.split(".")
      dates.append(split[0])
      dates_and_types.append({'date': split[0], 'type': split[1]})
      stats_map[split[0]] = []
    
    for date_and_type in dates_and_types:   # get this: {'20150506152026': ['source', 'variants'], '20150506151837': ['variants', 'source'], ... }
      stats_map[date_and_type['date']].append(date_and_type['type'])
    
    dates.sort(reverse=True)
    for date in dates:  # return the newest entry in stats_map that has both files (variants and source)
      if len(stats_map[date]) == 2:
        newest = ["{folder}/50_stats/{prefix}{date}.{type}.stats.json.gz".format(
            folder=study_folder, prefix=files_prefix, date=date, type=type) for type in stats_map[date] ]
        print "newest files : {news}".format(news=newest)
        
        return { 'variants' : luigi.LocalTarget(newest[0]),
                 'file'     : luigi.LocalTarget(newest[1]) }
    
    return {}
    #return { 'variants' : luigi.LocalTarget(files_root + '.variants.stats.json.gz'),
    #         'file'     : luigi.LocalTarget(files_root + '.source.stats.json.gz') }
 


class LoadVariantsStatistics(luigi.Task):

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
      #CreateVariantsStatistics(self.study_alias, self.study_name, self.study_description, self.study_uri, self.study_ticket_uri,
      #                     self.project_alias, self.project_name, self.project_description, self.project_organization)
    ]
  
  def run(self):
    create = CreateVariantsStatistics(self.path, self.database, self.study_alias, self.study_name, self.study_description, self.study_uri, self.study_ticket_uri,
                           self.project_alias, self.project_name, self.project_description, self.project_organization)
    files = create.output()

    print files
    print files['variants'].split(".")  # TODO luigi.LocalTarget is not a string, how to obtain the file name? 
    print files['variants'].split(".")[0:1]
    print "{stats_cohorts}.{date}".format(files['variants'].split(".")[0:1])
    
    
    
    config = configuration.get_opencga_config('pipeline_config.conf')
    command = 'echo {opencga-root}/bin/opencga.sh files stats-variants --user {user} --password {password} ' \
             '--cohort-id {cohort-id} --file-id "{user}@{project-alias}/{study-alias}/40_transformed/{filename}.MONGODB" ' \
             '--outdir-id "{user}@{project-alias}/{study-alias}/50_stats/" -- --load {stats_prefix} '
    kwargs = {'opencga-root'    : config['root_folder'],
             'user'            : config['catalog_user'],
             'password'        : config['catalog_pass'],
             'project-alias'   : self.project_alias,
             'study-alias'     : self.study_alias,
             'cohort-id'       : ",".join(create.get_cohort_ids()),
             'filename'        : os.path.basename(self.path),
             'stats_prefix'    : "{stats_cohorts}.{date}".format(files['variants'].split(".")[0:1])}
    
    shellout_no_stdout(command, **kwargs)


if __name__ == '__main__':
    luigi.run()
