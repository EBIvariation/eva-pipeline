package uk.ac.ebi.eva.test.configuration;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration;

@Configuration
@EnableBatchProcessing
@ComponentScan(basePackages = {"uk.ac.ebi.eva.pipeline.parameters"})
@Import({MongoConfiguration.class})
public class BatchTestConfiguration extends BaseTestConfiguration {

    @Bean
    public JobLauncherTestUtils jobLauncherTestUtils() {
        return new JobLauncherTestUtils();
    }

}
