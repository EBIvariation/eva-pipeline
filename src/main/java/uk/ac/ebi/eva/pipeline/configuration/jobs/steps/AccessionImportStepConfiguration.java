package uk.ac.ebi.eva.pipeline.configuration.jobs.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.PlatformTransactionManager;
import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.pipeline.configuration.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.readers.AccessionReportReaderConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.writers.AccessionImporterConfiguration;
import uk.ac.ebi.eva.pipeline.listeners.StepProgressListener;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ACCESSION_IMPORTER;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ACCESSION_IMPORT_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ACCESSION_REPORT_READER;

@Configuration
@Import({AccessionReportReaderConfiguration.class, AccessionImporterConfiguration.class, ChunkSizeCompletionPolicyConfiguration.class})
public class AccessionImportStepConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(AccessionImportStepConfiguration.class);
    @Autowired
    @Qualifier(ACCESSION_REPORT_READER)
    private ItemStreamReader<IVariant> reader;
    @Autowired
    @Qualifier(ACCESSION_IMPORTER)
    private ItemWriter<IVariant> writer;

    @Bean(ACCESSION_IMPORT_STEP)
    public Step accessionImportStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                    JobOptions jobOptions, SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        logger.debug("Building '" + ACCESSION_IMPORT_STEP + "'");

        return new StepBuilder(ACCESSION_IMPORT_STEP, jobRepository)
                .<IVariant, IVariant>chunk(chunkSizeCompletionPolicy, transactionManager)
                .reader(reader)
                .writer(writer)
                .faultTolerant()
                .allowStartIfComplete(jobOptions.isAllowStartIfComplete())
                .listener(new StepProgressListener())
                .build();
    }
}
