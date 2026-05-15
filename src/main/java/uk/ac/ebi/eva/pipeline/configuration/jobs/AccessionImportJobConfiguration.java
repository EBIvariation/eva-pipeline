package uk.ac.ebi.eva.pipeline.configuration.jobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.AccessionImportStepConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.NewJobIncrementer;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ACCESSION_IMPORT_JOB;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ACCESSION_IMPORT_STEP;

@Configuration
@EnableBatchProcessing
@Import({AccessionImportStepConfiguration.class})
public class AccessionImportJobConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(AccessionImportJobConfiguration.class);

    @Autowired
    @Qualifier(ACCESSION_IMPORT_STEP)
    private Step accessionImportStep;

    @Bean(ACCESSION_IMPORT_JOB)
    @Scope("prototype")
    public Job accessionImportJob(JobRepository jobRepository) {
        logger.debug("Building '" + ACCESSION_IMPORT_JOB + "'");

        return new JobBuilder(ACCESSION_IMPORT_JOB, jobRepository)
                .incrementer(new NewJobIncrementer())
                .start(accessionImportStep)
                .build();
    }
}
