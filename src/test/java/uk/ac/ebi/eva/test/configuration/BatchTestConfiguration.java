package uk.ac.ebi.eva.test.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import uk.ac.ebi.eva.pipeline.configuration.InMemoryBatchConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.AccessionImportJobConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.AggregatedVcfJobConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.AnnotationJobConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.DatabaseInitializationJobConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.DropStudyJobConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.FileStatsJobConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.GenotypedVcfJobConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.LoadVcfJobConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.VariantStatsJobConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.MongoConnectionDetails;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ACCESSION_IMPORT_JOB;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.AGGREGATED_VCF_JOB;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ANNOTATE_VARIANTS_JOB;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.DROP_STUDY_JOB;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.FILE_STATS_JOB;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.GENOTYPED_VCF_JOB;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.INIT_DATABASE_JOB;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.LOAD_VCF_JOB;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_STATS_JOB;


@Configuration
@ComponentScan(basePackages = {"uk.ac.ebi.eva.pipeline.parameters"})
@Import({InMemoryBatchConfiguration.class, MongoConfiguration.class, LoadVcfJobConfiguration.class,
        GenotypedVcfJobConfiguration.class, AggregatedVcfJobConfiguration.class, AccessionImportJobConfiguration.class,
        DatabaseInitializationJobConfiguration.class, AnnotationJobConfiguration.class,
        DropStudyJobConfiguration.class, VariantStatsJobConfiguration.class, FileStatsJobConfiguration.class})
public class BatchTestConfiguration extends BaseTestConfiguration {
    public static final String JOB_LOAD_VCF_JOB = "JOB_LOAD_VCF_JOB";
    public static final String JOB_GENOTYPE_VCF_JOB = "JOB_GENOTYPE_VCF_JOB";
    public static final String JOB_AGGREGATED_VCF_JOB = "JOB_AGGREGATED_VCF_JOB";
    public static final String JOB_ACCESSION_IMPORT_JOB = "JOB_ACCESSION_IMPORT_JOB";
    public static final String JOB_ANNOTATE_VARIANTS_JOB = "JOB_ANNOTATE_VARIANTS_JOB";
    public static final String JOB_INIT_DATABASE_JOB = "JOB_INIT_DATABASE_JOB";
    public static final String JOB_DROP_STUDY_JOB = "JOB_DROP_STUDY_JOB";
    public static final String JOB_VARIANT_STATS_JOB = "JOB_VARIANT_STATS_JOB";
    public static final String JOB_FILES_STATS_JOB = "JOB_FILES_STATS_JOB";

    @Value("${spring.data.mongodb.uri}")
    private String mongoDBUri;

    @Bean
    @Primary
    public MongoConnectionDetails mongoConnectionDetails() {
        MongoConnectionDetails details = new MongoConnectionDetails();
        details.setUri(mongoDBUri);
        return details;
    }

    public MongoTemplate getMongoTemplate(String dbName, MongoMappingContext mongoMappingContext) {
        MongoConnectionDetails mongoConnectionDetails = new MongoConnectionDetails();
        mongoConnectionDetails.setUri(mongoDBUri);
        return MongoConfiguration.getMongoTemplate(dbName, mongoConnectionDetails, mongoMappingContext);
    }

    @Bean(JOB_LOAD_VCF_JOB)
    public JobLauncherTestUtils loadVcfJobLauncherTestUtils(JobLauncher jobLauncher, JobRepository jobRepository,
                                                            @Qualifier(LOAD_VCF_JOB) Job job) {
        JobLauncherTestUtils utils = new JobLauncherTestUtils();
        utils.setJobLauncher(jobLauncher);
        utils.setJobRepository(jobRepository);
        utils.setJob(job);
        return utils;
    }

    @Bean(JOB_GENOTYPE_VCF_JOB)
    public JobLauncherTestUtils genotypeVCFJobLauncherTestUtils(JobLauncher jobLauncher, JobRepository jobRepository,
                                                                @Qualifier(GENOTYPED_VCF_JOB) Job job) {
        JobLauncherTestUtils utils = new JobLauncherTestUtils();
        utils.setJobLauncher(jobLauncher);
        utils.setJobRepository(jobRepository);
        utils.setJob(job);
        return utils;
    }

    @Bean(JOB_AGGREGATED_VCF_JOB)
    public JobLauncherTestUtils aggregatedVCFJobLauncherTestUtils(JobLauncher jobLauncher, JobRepository jobRepository,
                                                                  @Qualifier(AGGREGATED_VCF_JOB) Job job) {
        JobLauncherTestUtils utils = new JobLauncherTestUtils();
        utils.setJobLauncher(jobLauncher);
        utils.setJobRepository(jobRepository);
        utils.setJob(job);
        return utils;
    }

    @Bean(JOB_ACCESSION_IMPORT_JOB)
    public JobLauncherTestUtils accessionImportJobLauncherTestUtils(JobLauncher jobLauncher, JobRepository jobRepository,
                                                                    @Qualifier(ACCESSION_IMPORT_JOB) Job job) {
        JobLauncherTestUtils utils = new JobLauncherTestUtils();
        utils.setJobLauncher(jobLauncher);
        utils.setJobRepository(jobRepository);
        utils.setJob(job);
        return utils;
    }

    @Bean(JOB_INIT_DATABASE_JOB)
    public JobLauncherTestUtils initDBJobLauncherTestUtils(JobLauncher jobLauncher, JobRepository jobRepository,
                                                           @Qualifier(INIT_DATABASE_JOB) Job job) {
        JobLauncherTestUtils utils = new JobLauncherTestUtils();
        utils.setJobLauncher(jobLauncher);
        utils.setJobRepository(jobRepository);
        utils.setJob(job);
        return utils;
    }

    @Bean(JOB_ANNOTATE_VARIANTS_JOB)
    public JobLauncherTestUtils annotateVariantJobLauncherTestUtils(JobLauncher jobLauncher, JobRepository jobRepository,
                                                                    @Qualifier(ANNOTATE_VARIANTS_JOB) Job job) {
        JobLauncherTestUtils utils = new JobLauncherTestUtils();
        utils.setJobLauncher(jobLauncher);
        utils.setJobRepository(jobRepository);
        utils.setJob(job);
        return utils;
    }

    @Bean(JOB_DROP_STUDY_JOB)
    public JobLauncherTestUtils dropStudyJobLauncherTestUtils(JobLauncher jobLauncher, JobRepository jobRepository,
                                                              @Qualifier(DROP_STUDY_JOB) Job job) {
        JobLauncherTestUtils utils = new JobLauncherTestUtils();
        utils.setJobLauncher(jobLauncher);
        utils.setJobRepository(jobRepository);
        utils.setJob(job);
        return utils;
    }

    @Bean(JOB_VARIANT_STATS_JOB)
    public JobLauncherTestUtils variantStatsJobLauncherTestUtils(JobLauncher jobLauncher, JobRepository jobRepository,
                                                                 @Qualifier(VARIANT_STATS_JOB) Job job) {
        JobLauncherTestUtils utils = new JobLauncherTestUtils();
        utils.setJobLauncher(jobLauncher);
        utils.setJobRepository(jobRepository);
        utils.setJob(job);
        return utils;
    }

    @Bean(JOB_FILES_STATS_JOB)
    public JobLauncherTestUtils filesStatsJobLauncherTestUtils(JobLauncher jobLauncher, JobRepository jobRepository,
                                                               @Qualifier(FILE_STATS_JOB) Job job) {
        JobLauncherTestUtils utils = new JobLauncherTestUtils();
        utils.setJobLauncher(jobLauncher);
        utils.setJobRepository(jobRepository);
        utils.setJob(job);
        return utils;
    }

}
