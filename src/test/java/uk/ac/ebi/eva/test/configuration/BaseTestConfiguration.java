package uk.ac.ebi.eva.test.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.utils.MongoDBHelper;

@Configuration
@Import({ MongoDBHelper.class })
public class BaseTestConfiguration {

    @Bean
    public JobOptions jobOptions() {
        return new JobOptions();
    }

}
