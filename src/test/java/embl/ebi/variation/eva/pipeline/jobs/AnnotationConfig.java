package embl.ebi.variation.eva.pipeline.jobs;

import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

/**
 * @author Diego Poggioli
 *
 * Configuration init for Annotation Job
 */
@Configuration
@PropertySource({"annotation.properties"})
public class AnnotationConfig {
    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean
    public JobLauncherTestUtils jobLauncherTestUtils() {
                return new JobLauncherTestUtils();
           }
}
