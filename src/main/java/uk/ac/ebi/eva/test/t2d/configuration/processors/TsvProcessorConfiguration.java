package uk.ac.ebi.eva.test.t2d.configuration.processors;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.t2d.jobs.processors.TsvProcessor;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_TSV_PROCESSOR;

@Configuration
public class TsvProcessorConfiguration {

    @Bean(T2D_TSV_PROCESSOR)
    @StepScope
    public TsvProcessor tsvProcessor() {
        return new TsvProcessor();
    }

}
