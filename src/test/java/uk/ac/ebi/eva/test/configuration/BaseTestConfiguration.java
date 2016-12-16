package uk.ac.ebi.eva.test.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;

@Configuration
public class BaseTestConfiguration {

    @Bean
    public JobOptions jobOptions() {
        return new JobOptions();
    }

}
