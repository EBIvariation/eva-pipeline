package uk.ac.ebi.eva.test.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import uk.ac.ebi.eva.pipeline.parameters.EVAMongoConnectionDetails;

import static uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration.getMongoTemplate;

/**
 * Configuration to get a MongoOperations context tied to a specific static mongo database.
 */
@Configuration
public class MongoOperationConfiguration {

    private static final String DUMMY_STATIC = "dummy_test";

    @Bean
    public EVAMongoConnectionDetails mongoConnection() {
        return new EVAMongoConnectionDetails();
    }

    @Bean
    public MongoMappingContext mongoMappingContext() {
        return new MongoMappingContext();
    }

    @Bean
    public MongoOperations mongoTemplate(EVAMongoConnectionDetails EVAMongoConnectionDetails,
                                         MongoMappingContext mongoMappingContext) {
        return getMongoTemplate(DUMMY_STATIC, EVAMongoConnectionDetails, mongoMappingContext);
    }

}
