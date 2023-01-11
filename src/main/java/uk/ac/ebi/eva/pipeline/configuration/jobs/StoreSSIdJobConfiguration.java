package uk.ac.ebi.eva.pipeline.configuration.jobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.StoreSSIdStepConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.NewJobIncrementer;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.STORE_SS_ID_JOB;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.STORE_SS_ID_STEP;

@Configuration
@EnableBatchProcessing
@Import({StoreSSIdStepConfiguration.class})
public class StoreSSIdJobConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(StoreSSIdJobConfiguration.class);

    @Autowired
    @Qualifier(STORE_SS_ID_STEP)
    private Step storeSSIdStep;

    @Bean(STORE_SS_ID_JOB)
    @Scope("prototype")
    public Job storeSSIdJob(JobBuilderFactory jobBuilderFactory) {
        logger.debug("Building '" + STORE_SS_ID_JOB + "'");

        return jobBuilderFactory
                .get(STORE_SS_ID_JOB)
                .incrementer(new NewJobIncrementer())
                .start(storeSSIdStep)
                .build();
    }
}
