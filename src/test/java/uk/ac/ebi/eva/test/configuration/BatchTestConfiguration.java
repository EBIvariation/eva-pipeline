package uk.ac.ebi.eva.test.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration;
import uk.ac.ebi.eva.test.utils.StepLauncherTestUtils;

@Configuration
@EnableBatchProcessing
@ComponentScan(basePackages = {"uk.ac.ebi.eva.pipeline.parameters"})
@Import({MongoConfiguration.class})
public class BatchTestConfiguration extends BaseTestConfiguration {

    @Bean
    @ConditionalOnBean(value = {Job.class})
    public JobLauncherTestUtils jobLauncherTestUtils() {
        return new JobLauncherTestUtils();
    }

    @Bean
    @ConditionalOnBean(value = {Step.class})
    @ConditionalOnMissingBean(value = {Job.class})
    public StepLauncherTestUtils stepLauncherTestUtils() {
        return new StepLauncherTestUtils();
    }

}
