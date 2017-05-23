package uk.ac.ebi.eva.test.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import uk.ac.ebi.eva.pipeline.parameters.MongoConnection;

import java.net.UnknownHostException;

import static uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration.getMongoOperations;

/**
 * Configuration to get a MongoOperations context tied to a specific static mongo database.
 */
@Configuration
public class MongoOperationConfiguration {

    private static final String DUMMY_STATIC = "dummy_test";

    @Bean
    public MongoConnection mongoConnection() {
        return new MongoConnection();
    }

    @Bean
    public MongoMappingContext mongoMappingContext() {
        return new MongoMappingContext();
    }

    @Bean
    public MongoOperations mongoTemplate(MongoConnection mongoConnection, MongoMappingContext mongoMappingContext)
            throws UnknownHostException {
        return getMongoOperations(DUMMY_STATIC, mongoConnection, mongoMappingContext);
    }

}
