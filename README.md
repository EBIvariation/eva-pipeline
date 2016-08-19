# European Variation Archive (EVA) Pipeline v2 [![Build Status](https://travis-ci.org/EBIvariation/eva-pipeline.svg)](https://travis-ci.org/EBIvariation/eva-pipeline)

This repository contains work in progress for the next version of the European Variation Archive pipeline. If you are 
looking for the production source code, please check https://github.com/EBIvariation/eva-ws for the REST web services 
API, and the `master` branch of this same repository, https://github.com/EBIvariation/eva-pipeline.

The core of the new EVA is a pipeline for VCF file processing, implemented purely in Java and based on the Spring Batch 
framework, instead of combining the Luigi workflow manager with Java binaries. The reason for using Spring Batch is 
tracking job statuses and avoiding waste of computation, as result of repeating just the needed steps when something 
fails, in the more automated way possible.

## Features

The current goal is to allow indexing VCF files into MongoDB.

The approach is to have two different jobs: one for genotyped files, and another for aggregated files.

Both jobs will have four (logical) steps: transformation, loading, statistics and annotation.

## Using this tool

You may compile the project with `mvn package` and call the produced jar directly, as `$ java -jar eva-pipeline/target/eva-pipeline-0.1.jar`.

We did not implement a custom command line, we are using the
`org.springframework.boot.autoconfigure.batch.JobLauncherCommandLineRunner` class to obtain all the parameters from
command line. Almost all the parameters you can use are showed in the `example-launch.sh` script.

### Examples

Skeletons to load genotyped and aggregated VCF files are provided in the `examples` folder.

`application.properties` is used to configure database connections and applications the pipeline depends on: 
[OpenCGA](https://github.com/opencb/opencga/tree/hotfix/0.5) and [Ensembl VEP](http://www.ensembl.org/info/docs/tools/vep/index.html).

`load-genotyped-vcf.properties` and `load-aggregated-vcf.properties` are job-specific configurations.

If more convenient for your use case, the global and job configuration files can be merged into one.

It is likely that you will need to change some parameters to fit your installation and/or or configure your job. For instance, 
the location of your MongoDB databases, your OpenCGA/VEP installation directory, the folder were your files are, the type of job to run, etc.

By using these properties files, a job can be launched with a single command like:

    java -jar target/eva-pipeline-0.1.jar --spring.config.name=application,load-genotyped-vcf

Please note the `.properties` extension must not be specified.

The contents from the configuration files can be provided directly as command-line arguments, like the following:

    java -jar target/eva-pipeline-0.1.jar \
        --spring.batch.job.names=load-genotyped-vcf \
        input.vcf=/path/to/file.vcf \
        input.study.name=My sample study \
        ...
        app.vep.path=/path/to/variant-effect-predictor.pl

## Parameter reference

### Environment

* `spring.profiles.active`: "production" to keep track of half-executed jobs using a job repository database, "test" to use an in-memory database that will record a single run
* `app.opencga.path`: Path to the OpenCGA installation folder. An `ls` in that path should show the conf, analysis, bin and libs folders.
* `app.vep.path`: Path to the VEP installation folder.
* `app.vep.num-forks`: Number of processes to run VEP in parallel (recommended 4).

If using a persistent (not in-memory database), the following information needs to be filled in:

* `job.repository.driverClassName`: JDBC-specific argument that points to the database to use for the job repository (PostgreSQL tested and supported, driver name is `org.postgresql.Driver`)
* `job.repository.url`: JDBC database URL to connect to, including port and database name, such as `jdbc:postgresql://mydbhost:5432/dbname
* `job.repository.username`: Name of the user that will connect to the database
* `job.repository.password`: Password of the user that will connect to the database

Other parameters are:

* `config.db.read-preference`: In a distributed Mongo environment, replica to connect to (primary or secondary, default primary).
* `--logging.level.embl.ebi.variation.eva`: DEBUG, INFO, WARN, ERROR supported among others. Recommended DEBUG.
* `--logging.level.org.opencb.opencga`: Recommended DEBUG.
* `--logging.level.org.springframework`: Recommended INFO or WARN.


### General job tuning

* `--spring.batch.job.names`: The name of the job to run. At the moment it can be `load-genotyped-vcf`, `load-aggregated-vcf`, `annotate-variants` or `calculate-statistics`

Individual steps can be skipped using one of the following. This is not necessary unless some input data has been already generated in previous runs for the same job.

* `load.skip`
* `statistics.create.skip`
* `statistics.load.skip`
* `annotation.create.skip`

Other parameters are:

* `config.restartability.allow`: When set to `true`, it allows to restart a a job, even if partially run previously.


### Job run tuning

* `input.vcf`: Path to the VCF to process. May be compressed.
* `input.vcf.id`: Unique ID for the VCF to process. Could be an analysis in the SRA model (please ignore if you don't know what SRA is).
* `input.vcf.aggregation`: Whether aggregated statistics are provided in the VCF instead of the genotypes. NONE, BASIC, EXAC and EVS supported. NONE for genotyped files, BASIC for aggregated files in general.

* `input.study.id`: Unique ID for the study the file is associated with.
* `input.study.name`: Name of the study the file is associated with.
* `input.study.type`: Type of the study the file is associated with. COLLECTION, FAMILY, TRIO, CONTROL, CASE, CASE_CONTROL, PAIRED, PAIRED_TUMOR, TIME_SERIES and AGGREGATE supported.

* `input.pedigree`: PED file if available, in order to calculate population-based statistics.
* `input.fasta`: Path to the FASTA file with the reference sequence, in order to generate the VEP annotation.

* `output.dir`: Already existing folder to store the transformed VCF and statistics files.
* `output.dir.annotation`: Already existing folder to store VEP output files.

* `app.vep.cache.path`: Path to the VEP cache root folder.
* `app.vep.version`: Version of the VEP cache.
* `app.vep.species`: Name of the species as stored in the cache folder.
