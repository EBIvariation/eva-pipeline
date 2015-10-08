package embl.ebi.variation.eva;


import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.builder.TaskletStepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;

@Configuration
@EnableBatchProcessing
@EnableConfigurationProperties(PipelineConfig.class)
public class VariantConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(VariantConfiguration.class);
    public static final String jobName = "variantJob";

    @Autowired
    JobLauncher jobLauncher;
    @Autowired
    JobExplorer jobExplorer;
    @Autowired
    JobRepository jobRepository;
    @Autowired
    JobRegistry jobRegistry;

    @Autowired
    public PipelineConfig config;

    private VariantStorageManager variantStorageManager;
    private ObjectMap variantOptions;
    private URI outdirUri;
    private URI nextFileUri;
    private URI pedigreeUri;
    private Path output;
    private Path outputVariantJsonFile;
    private URI transformedVariantsUri;
//    private Path outputFileJsonFile;

    @Bean
    public Job variantJob(JobBuilderFactory jobs, JobExecutionListener listener, StepBuilderFactory stepBuilderFactory) {
        JobBuilder jobBuilder = jobs.get(jobName)
                .repository(jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener);


        SimpleJobBuilder simpleJobBuilder = jobBuilder.start(init(stepBuilderFactory))
                .next(transform(stepBuilderFactory))
                .next(load(stepBuilderFactory))
//                .next(stats(stepBuilderFactory))
//                .next(annotation(stepBuilderFactory));
                ;
        return simpleJobBuilder.build();
    }


    public Step init(StepBuilderFactory stepBuilderFactory) {
        StepBuilder step1 = stepBuilderFactory.get("initVariantJob");
        if (config == null) {
            throw new MissingResourceException("PipelineConfig not loaded. Hint: is the `application.properties` available? aborting...", "PipelineConfig", "pipelineConfig");
        }

        TaskletStepBuilder tasklet = step1.tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

                // TODO validation checks for all the parameters

                Config.setOpenCGAHome(config.appHome);

                // load configuration
                outdirUri = createUri(config.outputDir);
                pedigreeUri = createUri(config.pedigree);
                nextFileUri = createUri(config.input);
                String fileName = nextFileUri.resolve(".").relativize(nextFileUri).toString();
                VariantSource source = new VariantSource(config.input, config.fileId,
                        config.studyId, config.studyName, config.studyType, config.aggregated);

                variantOptions = new ObjectMap();
//                variantOptions.put(VariantStorageManager.STUDY_NAME.key(), "asdfasdfasdfasdf");
//                variantOptions.put(VariantStorageManager.STUDY_ID.key(), "asdfasdfasdfasdf");
//                variantOptions.put(VariantStorageManager.FILE_ID.key(), "asdfasdfasdfasdf");
//                variantOptions.put(VariantStorageManager.SAMPLE_IDS, Arrays.asList(config.samples.split(",")));
                variantOptions.put(VariantStorageManager.CALCULATE_STATS, false);
                variantOptions.put(VariantStorageManager.INCLUDE_STATS, false);
//                variantOptions.put(VariantStorageManager.INCLUDE_GENOTYPES.key(), false);   // TODO rename samples to genotypes
                variantOptions.put(VariantStorageManager.INCLUDE_SRC, VariantStorageManager.IncludeSrc.parse(config.includeSrc));
                variantOptions.put(VariantStorageManager.COMPRESS_GENOTYPES, config.compressGenotypes);
//                variantOptions.put(VariantStorageManager.AGGREGATED_TYPE, VariantSource.Aggregation.NONE);
                variantOptions.put(VariantStorageManager.DB_NAME, config.dbName);
                variantOptions.put(VariantStorageManager.ANNOTATE, false);
                variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);
                variantOptions.put("compressExtension", config.compressExtension);

                String storageEngine = config.storageEngine;
                variantStorageManager = StorageManagerFactory.getVariantStorageManager(storageEngine);

//                if (config.credentials != null && !config.credentials.isEmpty()) {
//                    variantStorageManager.addConfigUri(new URI(null, config.credentials, null));
//                }


                logger.debug("Using as variantOptions: {}", variantOptions.entrySet().toString());
                Path input = Paths.get(nextFileUri.getPath());
                output = Paths.get(outdirUri.getPath());
                outputVariantJsonFile = output.resolve(input.getFileName().toString() + ".variants.json" + config.compressExtension);
//                outputFileJsonFile = output.resolve(input.getFileName().toString() + ".file.json" + config.compressExtension);
                transformedVariantsUri = outdirUri.resolve(outputVariantJsonFile.getFileName().toString());

                return RepeatStatus.FINISHED;
            }
        });


        // true: every job execution will do this step, even if this step is COMPLETED
        // false: if the job was aborted and is relaunched, this step will NOT be done again
        tasklet.allowStartIfComplete(true);

        return tasklet.build();
    }


    public Step transform(StepBuilderFactory stepBuilderFactory) {
        StepBuilder step1 = stepBuilderFactory.get("transform");
        TaskletStepBuilder tasklet = step1.tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                logger.info("transform file " + config.input + " to " + config.outputDir);

                logger.info("Extract variants '{}'", nextFileUri);
                variantStorageManager.extract(nextFileUri, outdirUri, variantOptions);

                logger.info("PreTransform variants '{}'", nextFileUri);
                variantStorageManager.preTransform(nextFileUri, variantOptions);
                logger.info("Transform variants '{}'", nextFileUri);
                variantStorageManager.transform(nextFileUri, pedigreeUri, outdirUri, variantOptions);
                logger.info("PostTransform variants '{}'", nextFileUri);
                variantStorageManager.postTransform(nextFileUri, variantOptions);
                return RepeatStatus.FINISHED;
            }
        });

        // true: every job execution will do this step, even if this step is COMPLETED
        // false: if the job was aborted and is relaunched, this step will NOT be done again
        tasklet.allowStartIfComplete(false);

        return tasklet.build();
    }


    public Step load(StepBuilderFactory stepBuilderFactory) {
        StepBuilder step1 = stepBuilderFactory.get("load");
        TaskletStepBuilder tasklet = step1.tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

                logger.info("-- PreLoad variants -- {}", nextFileUri);
                variantStorageManager.preLoad(transformedVariantsUri, outdirUri, variantOptions);
                logger.info("-- Load variants -- {}", nextFileUri);
                variantStorageManager.load(transformedVariantsUri, variantOptions);
                logger.info("-- PostLoad variants -- {}", nextFileUri);
                variantStorageManager.postLoad(transformedVariantsUri, outdirUri, variantOptions);
                return RepeatStatus.FINISHED;
            }
        });


        // true: every job execution will do this step, even if this step is COMPLETED
        // false: if the job was aborted and is relaunched, this step will NOT be done again
        tasklet.allowStartIfComplete(false);
        return tasklet.build();
    }

    public static URI createUri(String input) throws URISyntaxException {
        URI sourceUri = new URI(null, input, null);
        if (sourceUri.getScheme() == null || sourceUri.getScheme().isEmpty()) {
            sourceUri = Paths.get(input).toUri();
        }
        return sourceUri;
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        return jdbcTemplate;
    }

    @Bean
    public DataSource postgresDataSource(PipelineConfig pipelineConfig) throws SQLException {
        final DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(pipelineConfig.jobRepositoryDriverClassName);
        dataSource.setUrl(pipelineConfig.jobRepositoryUrl);
        dataSource.setUsername(pipelineConfig.jobRepositoryUsername);
        dataSource.setPassword(pipelineConfig.jobRepositoryPassword);
        logger.info("using as repository url: " + pipelineConfig.jobRepositoryUrl);
        return dataSource;

    }


    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
        jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry);
        return jobRegistryBeanPostProcessor;
    }

}
