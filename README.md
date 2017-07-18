# European Variation Archive (EVA) Pipeline v2 [![Build Status](https://travis-ci.org/EBIvariation/eva-pipeline.svg)](https://travis-ci.org/EBIvariation/eva-pipeline)

The European Variation Archive pipeline processes Variant Call Format (VCF) files and stores the variants contained within them in a database, in a format that supports efficient searching. The EVA pipeline produces and stores the following information:

* A normalized representation of the variants contained within a VCF file
* Variant annotation: consequence type, SIFT and Polyphen scores, etc
* Statistics: allele and genotype counts and frequencies

You can find a more detailed description of these operations in the [project wiki](https://github.com/EBIvariation/eva-pipeline/wiki/Jobs). Please visit the [EVA website](http://www.ebi.ac.uk/eva/?Variant Browser) to see a public service depending on this pipeline, and the [EVA web services repository](https://github.com/EBIvariation/eva-ws) for more information on the API.

The pipeline automatically tracks the job status, and avoids waste of computation by resuming a job in the exact point where it failed; successful steps already executed are automatically skipped.

## Dependencies

The pipeline has been implemented in Java and uses the Maven build system.

In order to run, the pipeline needs access to a MongoDB 3.x database instance. The easiest way to set one up in a local machine is [using Docker](https://hub.docker.com/_/mongo/).

If you want to generate and store variant annotations you will also need to [download Ensembl VEP](http://www.ensembl.org/info/docs/tools/vep/script/vep_download.html). Please note this software requires Perl to be installed.

## Build

The latest stable version can be found in the [master](https://github.com/EBIvariation/eva-pipeline/tree/master) branch. [develop](https://github.com/EBIvariation/eva-pipeline/tree/develop) contains work in progress, which is fully tested but could be more unstable.

If a MongoDB instance is available in the machine where you are running the build, you can test and build the application with `mvn test package`, otherwise please run `mvn package -DskipTests`.

## Run

Arguments to run the pipeline can be provided either using the command line or property files. The `examples` folder contains skeletons for configuring the environment and executing jobs to load genotyped and aggregated VCF files and to drop studies from the database.

`application.properties` is used to configure database connections and applications the pipeline depends on (OpenCGA and Ensembl VEP, see _Dependencies_ section).

`load-genotyped-vcf.properties`, `load-aggregated-vcf.properties` , `drop-study-job.properties` and `initialize-database.properties` are job-specific configurations.

If more convenient for your use case, the global configuration and job parameters files can be merged into one.

It is likely that you will need to edit some parameters to match your environment and/or configure your job. For instance, connection details to MongoDB databases, OpenCGA/VEP installation directories, the folder containing the input files, the type of job to run, etc.

**Note:** Most of the environment configuration can be provided directly to the application, but MongoDB connection details also need to be filled in the OpenCGA configuration file. This configuration file must be located in `<OpenCGA configuration folder>/conf/storage-mongodb.properties`, and the OpenCGA configuration folder must be specified in the variable `app.opencga.path=/path/to/opencga` in your `application.properties`. The root directory of the eva-pipeline repository can be used as OpenCGA configuration folder (as it contains the `conf` folder with the configuration files), but you can use any folder that contains the `conf` directory with the same files.

By using these properties files, a job can be launched with a single command like:

    java -jar target/eva-pipeline-2.0-beta2-SNAPSHOT.jar \
        --spring.config.location=file:examples/application.properties --parameters.path=file:examples/load-genotyped-vcf.properties

The contents from the configuration files can be also provided directly as command-line arguments, like the following:

    java -jar target/eva-pipeline-2.0-beta2-SNAPSHOT.jar \
        --spring.batch.job.names=load-genotyped-vcf \
        --input.vcf=/path/to/file.vcf \
        --input.study.name=My sample study \
        ...
        --app.vep.path=/path/to/variant-effect-predictor.pl

## Parameter reference

### Environment

* `spring.profiles.active`: "production" to keep track of half-executed jobs using a job repository database, "test" to use an in-memory database that will record a single run
* `app.opencga.path`: Path to the OpenCGA configuration folder. The eva-pipeline root folder is a valid path. Remember to configure there your database connection in the file `conf/storage-mongodb.properties`.

Database credentials used to connect to a MongoDB instance. See [MongoDB options documentation](https://docs.mongodb.com/manual/reference/program/mongo/#options). The database and collection names are listed below in the "Database parameters" section.

* `spring.data.mongodb.authentication-database`
* `spring.data.mongodb.host`
* `spring.data.mongodb.password`
* `spring.data.mongodb.port`
* `spring.data.mongodb.username`

If using a persistent (not in-memory database), the following information needs to be filled in:

* `job.repository.driverClassName`: JDBC-specific argument that points to the database to use for the job repository (PostgreSQL tested and supported, driver name is `org.postgresql.Driver`)
* `job.repository.url`: JDBC database URL to connect to, including port and database name, such as `jdbc:postgresql://mydbhost:5432/dbname
* `job.repository.username`: Name of the user that will connect to the database
* `job.repository.password`: Password of the user that will connect to the database

Other parameters are:

* `config.db.read-preference`: In a distributed Mongo environment, replica to connect to (primary or secondary, default primary).
* `logging.level.uk.ac.ebi.eva`: DEBUG, INFO, WARN, ERROR supported among others. Recommended DEBUG.
* `logging.level.org.opencb.opencga`: Recommended DEBUG.
* `logging.level.org.springframework`: Recommended INFO or WARN.


### Job parameters

#### Job configuration

* `spring.batch.job.names`: The name of the job to run. At the moment it can be `genotyped-vcf-job`, `aggregated-vcf-job`, `annotate-variants-job`, `calculate-statistics-job` or `drop-study-job`

Individual steps can be skipped using one of the following. This is not necessary unless they are irrelevant for the data to be processed, or some input data was generated in previous runs of the same job.

* `statistics.skip`
* `annotation.skip`

Other parameters are:

* `config.chunk.size`: Size of batches across the pipeline (recommended from 100 to 5000).
* `annotation.overwrite`: True to overwrite annotations already associated to variants. False to annotate only variants without an existing annotation. Please note that if the `input.study.id` parameter is specified, annotation will be limited to variants from that study.
* `force.restart`: When included as command line parameter allows to restart a a job. This will also mark the last execution not finished of the same job / parameters as cancelled in the job database.

#### Job inputs

* `input.vcf`: Path to the VCF to process. May be compressed.
* `input.vcf.id`: Unique ID for the VCF to process. Could be an analysis in the SRA model (please ignore if you don't know what SRA is).
* `input.vcf.aggregation`: Whether aggregated statistics are provided in the VCF instead of the genotypes. NONE, BASIC, EXAC and EVS supported. NONE for genotyped files, BASIC for aggregated files in general.

* `input.study.id`: Unique ID for the study the file is associated with.
* `input.study.name`: Name of the study the file is associated with.
* `input.study.type`: Type of the study the file is associated with. COLLECTION, FAMILY, TRIO, CONTROL, CASE, CASE_CONTROL, PAIRED, PAIRED_TUMOR, TIME_SERIES and AGGREGATE supported.

* `input.pedigree`: PED file if available, in order to calculate population-based statistics.
* `input.fasta`: Path to the FASTA file with the reference sequence, in order to generate the VEP annotation.

#### Job outputs

* `output.dir`: Already existing folder to store the transformed VCF and statistics files.
* `output.dir.annotation`: Already existing folder to store VEP output files.
* `output.dir.statistics`: Already existing folder to store statistics output files.

#### Database parameters

Database name and collection names can be specified with these parameters. To set the database credentials, use the parameters in the "Environment" section.

* `spring.data.mongodb.database`: Database name, that contain all the collections
* `db.collections.variants.name`: Main collection. Has variant coordinates, sample information, and some statistics and annotation.
* `db.collections.files.name`: File (and study) metadata information.
* `db.collections.stats.name`: Main collection for statistics. The variants collection might contain a subset of this.
* `db.collections.annotation-metadata.name`: Main collection for annotation. The variants collection might contain a subset of this.

#### Configuration of third party applications

* `app.vep.cache.path`: Path to the VEP cache root folder.
* `app.vep.version`: Version of the VEP executable.
* `app.vep.cache.version`: Version of the VEP cache.
* `app.vep.species`: Name of the species as stored in the cache folder.
* `app.vep.path`: Path to the VEP installation folder.
* `app.vep.num-forks`: Number of processes to run VEP in parallel (recommended 4).
* `app.vep.timeout`: If VEP doesn't respond in the specified number of seconds, the pipeline will assume that the step failed (recommended 300).
