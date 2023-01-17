package uk.ac.ebi.eva.pipeline.configuration.jobs.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.configuration.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.readers.AccessionReportReaderConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.io.writers.AccessionImporterConfiguration;
import uk.ac.ebi.eva.pipeline.listeners.StepProgressListener;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ACCESSION_IMPORT_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ACCESSION_REPORT_READER;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ACCESSION_IMPORTER;

@Configuration
@EnableBatchProcessing
@Import({AccessionReportReaderConfiguration.class, AccessionImporterConfiguration.class, ChunkSizeCompletionPolicyConfiguration.class})
public class AccessionImportStepConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(AccessionImportStepConfiguration.class);
    @Autowired
    @Qualifier(ACCESSION_REPORT_READER)
    private ItemStreamReader<Variant> reader;
    @Autowired
    @Qualifier(ACCESSION_IMPORTER)
    private ItemWriter<Variant> writer;

    @Bean(ACCESSION_IMPORT_STEP)
    public Step accessionImportStep(StepBuilderFactory stepBuilderFactory, JobOptions jobOptions,
                              SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        logger.debug("Building '" + ACCESSION_IMPORT_STEP + "'");

        return stepBuilderFactory.get(ACCESSION_IMPORT_STEP)
                .<Variant, Variant>chunk(chunkSizeCompletionPolicy)
                .reader(reader)
                .writer(writer)
                .faultTolerant()
                .allowStartIfComplete(jobOptions.isAllowStartIfComplete())
                .listener(new StepProgressListener())
                .build();
    }
}
