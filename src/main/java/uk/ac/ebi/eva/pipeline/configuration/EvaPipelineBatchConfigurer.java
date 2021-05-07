package uk.ac.ebi.eva.pipeline.configuration;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.commons.batch.configuration.SpringBoot1CompatibilityConfiguration;

@Configuration
@EnableBatchProcessing
public class EvaPipelineBatchConfigurer {

    @Bean
    public BatchConfigurer configurer(DataSource dataSource, EntityManagerFactory entityManagerFactory)
            throws Exception {
        return SpringBoot1CompatibilityConfiguration.getSpringBoot1CompatibleBatchConfigurer(dataSource,
                entityManagerFactory);
    }
}
