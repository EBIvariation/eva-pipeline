# European Variation Archive (EVA) Pipeline v2 [![Build Status](https://travis-ci.org/EBIvariation/eva-pipeline.svg)](https://travis-ci.org/EBIvariation/eva-pipeline)

The European Variation Archive pipeline processes Variant Call Format (VCF) files and stores the variants contained within them in a database, in a format that supports efficient searching. The EVA pipeline produces and stores the following information:

* A normalized representation of the variants contained within a VCF file
* Variant annotation: consequence type, SIFT and Polyphen scores, etc
* Statistics: allele and genotype counts and frequencies

You can find a more detailed description of these operations in the [project wiki](https://github.com/EBIvariation/eva-pipeline/wiki/Jobs). Please visit the [EVA website](http://www.ebi.ac.uk/eva/?Variant Browser) to see a public service depending on this pipeline, and the [EVA web services repository](https://github.com/EBIvariation/eva-ws) for more information on the API.

The pipeline automatically tracks the job status, and avoids waste of computation by resuming a job in the exact point where it failed; successful steps already executed are automatically skipped.

## Dependencies

The pipeline has been implemented in Java and uses the Maven build system.

In order to run, the pipeline needs access to a MongoDB database instance. The easiest way to set one up in a local machine is [using Docker](https://hub.docker.com/_/mongo/).

If you want to generate and store variant annotations you will also need to [download Ensembl VEP](http://www.ensembl.org/info/docs/tools/vep/script/vep_download.html). Please note this software requires Perl to be installed.

Finally, before compiling the pipeline itself, you will need to clone and build its dependencies running these commands from a folder of your choice:

```
git clone https://github.com/EBIvariation/biodata.git
cd biodata && mvn clean install -DskipTests
cd ..
git clone https://github.com/EBIvariation/opencga.git
cd opencga && mvn clean install -DskipTests
```

### Build

The latest stable version can be found in the [master](https://github.com/EBIvariation/eva-pipeline/tree/master) branch. [develop](https://github.com/EBIvariation/eva-pipeline/tree/develop) contains work in progress, which is fully tested but could be more unstable.

If a MongoDB instance is available in the machine where you are running the build, you can test and build the application with `mvn test package`, otherwise please run `mvn package -DskipTests`.

## Run

Once successfully built, you can simply run the produced JAR file with `java -jar target/eva-pipeline-2.0-beta2-SNAPSHOT.jar`.

Arguments to run the pipeline can be provided either using the command line or a properties file. Skeletons to load genotyped and aggregated VCF files are provided in the `examples` folder.

`application.properties` is used to configure database connections and applications the pipeline depends on (OpenCGA and Ensembl VEP, see _Dependencies_ section).

`load-genotyped-vcf.properties`, `load-aggregated-vcf.properties` and `initialize-database.properties` are job-specific configurations.

If more convenient for your use case, the global configuration and job parameters files can be merged into one.

It is likely that you will need to change some parameters to fit your installation and/or or configure your job. For instance, 
the location of your MongoDB databases, your OpenCGA/VEP installation directory, the folder were your files are, the type of job to run, etc.

By using these properties files, a job can be launched with a single command like:

    java -jar target/eva-pipeline-2.0-beta2-SNAPSHOT.jar \
        --spring.config.location=file:examples/application.properties,file:examples/load-genotyped-vcf.properties

The contents from the configuration files can be provided directly as command-line arguments, like the following:

    java -jar target/eva-pipeline-2.0-beta2-SNAPSHOT.jar \
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
* `--logging.level.uk.ac.ebi.eva`: DEBUG, INFO, WARN, ERROR supported among others. Recommended DEBUG.
* `--logging.level.org.opencb.opencga`: Recommended DEBUG.
* `--logging.level.org.springframework`: Recommended INFO or WARN.


### General job tuning

* `--spring.batch.job.names`: The name of the job to run. At the moment it can be `load-genotyped-vcf`, `load-aggregated-vcf`, `annotate-variants` or `calculate-statistics`

Individual steps can be skipped using one of the following. This is not necessary unless they are irrelevant for the data to be processed, or some input data was generated in previous runs of the same job.

* `statistics.skip`
* `annotation.skip`

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
* `output.dir.statistics`: Already existing folder to store statistics output files.

* `app.vep.cache.path`: Path to the VEP cache root folder.
* `app.vep.version`: Version of the VEP cache.
* `app.vep.species`: Name of the species as stored in the cache folder.
