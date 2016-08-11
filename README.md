# European Variation Archive (EVA) v2 [![Build Status](https://travis-ci.org/EBIvariation/eva-v2.svg)](https://travis-ci.org/EBIvariation/eva-v2)

This repository contains work in progress for the next version of the European Variation Archive. If you are looking for the production source code, please check https://github.com/EBIvariation/eva and https://github.com/EBIvariation/eva-pipeline

Its components form the core of the new EVA:

* The pipeline for VCF file processing, based on the Spring Batch framework
* The database access layer

The web services and metadata components will be developed in independent repositories.


## EVA Pipeline
#### Using OpenCGA and Spring batch

The current goal is to allow indexing VCF files into mongoDB.

The approach is to have two different jobs: one for genotyped files, and another for aggregated files.

Both jobs will have four (logical) steps: transformation, loading, statistics and annotation.

The reason for using Spring Batch is tracking job statuses and avoiding waste of computation, as result of repeating just 
the needed steps when something fails, in the more automated way possible.

### Using this tool

You may compile the project with `mvn package` and call the produced jar directly, as `$ java -jar eva-pipeline/target/eva-pipeline-0.1.jar`.

We did not implement a custom command line, we are using the
`org.springframework.boot.autoconfigure.batch.JobLauncherCommandLineRunner` class to obtain all the parameters from
command line. Almost all the parameters you can use are showed in the `example-launch.sh` script.

#### Examples

Working examples such as `example-launch.sh` are provided to launch directly as:
`/example-launch.sh`. It is likely that you need to change some parameters to fit your installation
or configure your job. For instance: the location of your MongoDB databases, your OpenCGA
installation directory, the folder were your files are, the type of job to run, etc.

Below there is an example parameter configuration: specifying the job to run. There is the `spring.batch.job.names`
parameter. You can choose from `[aggregatedVariantJob, variantLoadJob, variantStatsJob, variantJob]`, and specify it in
the command line, like this:

    java -jar eva-pipeline/target/eva-pipeline-0.1.jar \
        --spring.batch.job.names=variantJob \
        ...
        dbname=batch \
        ...
        opencga.app.home=/opt/opencga

### Parameter reference

#### Installation

* `opencga.app.home`: path to the installation of OpenCGA. An `ls` in that path should show the conf, analysis, bin and libs folders.
* `storageEngine`: currently, only mongodb is tested.

#### Coarse job tuning
* `--spring.batch.job.names`: Several jobs are available. Allowed values:  
    * `variantJob`
    * `aggregatedVariantJob`
    * `variantLoadJob`
    * `variantStatsJob`
    * `variantAnnotJob`


* `skipLoad`: As all the other `skip*` parameter, skips optional steps. Assign `true` to skip a step. Default value is
 `false`, so all steps will be done if unspecified. Note that the transform step is not skippable, it's almost sure
 that what you need is another job.
* `skipStatsCreate`
* `skipStatsLoad`
* `skipAnnotGenerateInput`
* `skipAnnotCreate`
* `skipAnnotLoad`


* `--logging.level.embl.ebi.variation.eva`: Allowed values: [ERROR, INFO, DEBUG] among others. Any other package may be also specified. Recommended DEBUG.
* `--logging.level.org.opencb.opencga`: Recommended DEBUG.
* `--logging.level.org.springframework`: Recommended INFO.

#### Fine job tuning (input dependent)
* `input`: path to the desired VCF to process. May be compressed.
* `outputdir`: folder to store the output files that will be later loaded, like the transformed VCF or statistics files. Must exist.
* `dbName`: database name to load the variants and file.
* `overwriteStats`: boolean. Overwrite previously computed and loaded stats.
* `compressGenotypes`: boolean.
* `compressExtension`: usually `.gz`.
* `includeSrc`: Allowed values: [NO, FIRST_8_COLUMNS, FULL]. See org.opencb.opencga.storage.core.variant.VariantStorageManager.IncludeSrc.

* `vepInput`: Path where the future input for VEP, i.e. the list of variants to annotate, will be generated.
* `vepOutput`: Path to the file that will contain the annotated variants.
* `vepPath`: Full path to the `variant_effect_predictor.pl` script.
* `vepParameters`: See http://www.ensembl.org/info/docs/tools/vep/script/vep_options.html
* `vepCacheDirectory`: Path to the VEP cache root folder.
* `vepCacheVersion`: Version of the VEP cache.
* `vepSpecies`: Name of the species as stored in the cache folder.
* `vepFasta`: Path to the FASTA file with the reference sequence.
* `vepNumForks`: Number of forks to run VEP concurrently (recommended 4).

#### Metadata
* `studyId`: unique identifier of the study.
* `fileId`: unique identifier of the file.
* `studyName`: string of a human-readable name.
* `studyType`: Allowed values: [COLLECTION, FAMILY, TRIO, CONTROL, CASE, CASE_CONTROL, PAIRED, PAIRED_TUMOR, TIME_SERIES, AGGREGATE]. See org.opencb.biodata.models.variant.VariantStudy.StudyType
* `aggregated`: Allowed values: [NONE, BASIC, EVS, EXAC]. See org.opencb.biodata.models.variant.VariantSource.Aggregation
* `pedigree`: not implemented.

