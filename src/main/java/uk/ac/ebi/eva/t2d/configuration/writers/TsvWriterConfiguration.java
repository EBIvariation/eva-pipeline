package uk.ac.ebi.eva.t2d.configuration.writers;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.t2d.jobs.writers.TsvWriter;
import uk.ac.ebi.eva.t2d.services.T2dService;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_TSV_WRITER;

@Configuration
@Profile(Application.T2D_PROFILE)
public class TsvWriterConfiguration {

    @Bean(T2D_TSV_WRITER)
    @StepScope
    public TsvWriter tsvWriter(T2dService t2dService) {
        return new TsvWriter(t2dService);
    }

}
