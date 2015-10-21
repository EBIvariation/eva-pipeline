package embl.ebi.variation.eva;

import embl.ebi.variation.eva.pipeline.configuration.InfrastructureConfiguration;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.builder.TaskletStepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Configuration
@PropertySource("classpath:application.properties")
public class VariantConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(VariantConfiguration.class);
    public static final String jobName = "variantJob";

    @Autowired
    private InfrastructureConfiguration infrastructureConfiguration;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    PipelineConfig config;

    private VariantStorageManager variantStorageManager;
    private ObjectMap variantOptions;
    private URI outdirUri;
    private URI nextFileUri;
    private URI pedigreeUri;
    private Path output;
    private Path outputVariantJsonFile;
    private URI transformedVariantsUri;
    private Map<String, Set<String>> samples;
    private URI statsOutputUri;
    //    private Path outputFileJsonFile;

    @Bean
    public Job variantJob() {
        JobBuilder jobBuilder = jobBuilderFactory.get(jobName)
                .incrementer(new RunIdIncrementer())
//                .listener(listener)
                ;

        return jobBuilder
                .start(init())
                .next(transform())
                .next(load())
                .next(statsCreate())
                .next(statsLoad())
//                .next(annotation(stepBuilderFactory));
                .build();
    }

    public Step init() {
        StepBuilder step1 = stepBuilderFactory.get("initVariantJob");
        if (config == null) {
            throw new MissingResourceException("PipelineConfig not loaded. Hint: is the `application.properties` available? aborting...", "PipelineConfig", "pipelineConfig");
        }

        TaskletStepBuilder tasklet = step1.tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

                // TODO validation checks for all the parameters
                // TODO get samples
//                System.out.println(config.samples);
//                System.out.println(config.samples.size());
//                if (config.samples.size() != -3) {
//                    throw new Exception("aborting");
//                }
                Config.setOpenCGAHome(config.appHome);

                // load configuration
                outdirUri = createUri(config.outputDir);
                pedigreeUri = createUri(config.pedigree);
                nextFileUri = createUri(config.input);
                String fileName = nextFileUri.resolve(".").relativize(nextFileUri).toString();
                VariantSource source = new VariantSource(config.input, config.fileId,
                        config.studyId, config.studyName, config.studyType, config.aggregated);

                variantOptions = new ObjectMap();
//                variantOptions.put(VariantStorageManager.SAMPLE_IDS, Arrays.asList(config.samples.split(",")));
                variantOptions.put(VariantStorageManager.CALCULATE_STATS, false);   // this is tested by hand
                variantOptions.put(VariantStorageManager.OVERWRITE_STATS, config.overwriteStats);

                variantOptions.put(VariantStorageManager.INCLUDE_STATS, false);
//                variantOptions.put(VariantStorageManager.INCLUDE_GENOTYPES.key(), false);   // TODO rename samples to genotypes
                variantOptions.put(VariantStorageManager.INCLUDE_SAMPLES, true);   // TODO rename samples to genotypes
                variantOptions.put(VariantStorageManager.INCLUDE_SRC, VariantStorageManager.IncludeSrc.parse(config.includeSrc));
                variantOptions.put(VariantStorageManager.COMPRESS_GENOTYPES, config.compressGenotypes);
//                variantOptions.put(VariantStorageManager.AGGREGATED_TYPE, VariantSource.Aggregation.NONE);
                variantOptions.put(VariantStorageManager.DB_NAME, config.dbName);
                variantOptions.put(VariantStorageManager.ANNOTATE, false);
                variantOptions.put(MongoDBVariantStorageManager.LOAD_THREADS, config.loadThreads);
                variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);
                variantOptions.put("compressExtension", config.compressExtension);

                String storageEngine = config.storageEngine;
                variantStorageManager = StorageManagerFactory.getVariantStorageManager(storageEngine);

//                if (config.credentials != null && !config.credentials.isEmpty()) {
//                    variantStorageManager.addConfigUri(new URI(null, config.credentials, null));
//                }
                logger.debug("Using as variantOptions: {}", variantOptions.entrySet().toString());
                logger.debug("Using as input: {}", config.input);
                Path input = Paths.get(nextFileUri.getPath());
                output = Paths.get(outdirUri.getPath());
                outputVariantJsonFile = output.resolve(input.getFileName().toString() + ".variants.json" + config.compressExtension);
//                outputFileJsonFile = output.resolve(input.getFileName().toString() + ".file.json" + config.compressExtension);
                transformedVariantsUri = outdirUri.resolve(outputVariantJsonFile.getFileName().toString());

                // stats config
//                statsOutputUri = outdirUri.resolve(VariantStorageManager.buildFilename(source) + "." + TimeUtils.getTime());  // TODO why was the timestamp required?
                statsOutputUri = outdirUri.resolve(VariantStorageManager.buildFilename(source));
                return RepeatStatus.FINISHED;
            }
        });

        // true: every job execution will do this step, even if this step is already COMPLETED
        // false: if the job was aborted and is relaunched, this step will NOT be done again
        tasklet.allowStartIfComplete(true);

        return tasklet.build();
    }

    public Step transform() {
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

        // true: every job execution will do this step, even if this step is already COMPLETED
        // false: if the job was aborted and is relaunched, this step will NOT be done again
        tasklet.allowStartIfComplete(false);

        return tasklet.build();
    }

    public Step load() {
        StepBuilder step1 = stepBuilderFactory.get("load");
        TaskletStepBuilder tasklet = step1.tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

                logger.info("-- PreLoad variants -- {}", nextFileUri);
                variantStorageManager.preLoad(transformedVariantsUri, outdirUri, variantOptions);
                logger.info("-- Load variants -- {}", nextFileUri);
                variantStorageManager.load(transformedVariantsUri, variantOptions);
//                logger.info("-- PostLoad variants -- {}", nextFileUri);
//                variantStorageManager.postLoad(transformedVariantsUri, outdirUri, variantOptions);
                return RepeatStatus.FINISHED;
            }
        });

        // true: every job execution will do this step, even if this step is already COMPLETED
        // false: if the job was aborted and is relaunched, this step will NOT be done again
        tasklet.allowStartIfComplete(false);
        return tasklet.build();
    }

    public Step statsCreate() {
        StepBuilder step1 = stepBuilderFactory.get("statsCreate");
        TaskletStepBuilder tasklet = step1.tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                samples = new HashMap<String, Set<String>>(); // TODO fill properly. if this is null overwrite will take on
                samples.put("SOME", new HashSet<String>(Arrays.asList("HG00096", "HG00097")));

                if (config.calculateStats) { // TODO maybe this `if` is skippable with job flows
                    // obtaining resources. this should be minimum, in order to skip this step if it is completed
                    VariantStatisticsManager variantStatisticsManager = new VariantStatisticsManager();
                    QueryOptions statsOptions = new QueryOptions(variantOptions);
                    VariantSource variantSource = variantOptions.get(VariantStorageManager.VARIANT_SOURCE, VariantSource.class);
                    VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(config.dbName, variantOptions);

                    // actual stats creation
                    variantStatisticsManager.createStats(dbAdaptor, statsOutputUri, samples, statsOptions);
                } else {
                    logger.info("skipping stats creation");
                }

                return RepeatStatus.FINISHED;
            }
        });

        // true: every job execution will do this step, even if this step is already COMPLETED
        // false: if the job was aborted and is relaunched, this step will NOT be done again
        tasklet.allowStartIfComplete(false);
        return tasklet.build();
    }

    public Step statsLoad() {
        StepBuilder step1 = stepBuilderFactory.get("statsLoad");
        TaskletStepBuilder tasklet = step1.tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

                if (config.calculateStats) {
                    // obtaining resources. this should be minimum, in order to skip this step if it is completed
                    VariantStatisticsManager variantStatisticsManager = new VariantStatisticsManager();
                    QueryOptions statsOptions = new QueryOptions(variantOptions);
                    VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(config.dbName, variantOptions);

                    // actual stats load
                    variantStatisticsManager.loadStats(dbAdaptor, statsOutputUri, statsOptions);
                } else {
                    logger.info("skipping stats loading");
                }

                return RepeatStatus.FINISHED;
            }
        });

        // true: every job execution will do this step, even if this step is already COMPLETED
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
    public PipelineConfig pipelineConfig() {
        return new PipelineConfig();
    }

    // To resolve ${} in @Value
    @Bean
    public static PropertySourcesPlaceholderConfigurer propertyConfigInDev() {
        return new PropertySourcesPlaceholderConfigurer();
    }
}
