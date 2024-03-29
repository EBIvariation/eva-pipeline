package uk.ac.ebi.eva.test.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import uk.ac.ebi.eva.pipeline.parameters.MongoConnectionDetails;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;

import static uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration.getMongoTemplate;

/**
 * Configuration to get a MongoOperations context tied to a specific static mongo database.
 */
@Configuration
public class MongoOperationConfiguration {

    private static final String DUMMY_STATIC = "dummy_test";

    @Bean
    public MongoConnectionDetails mongoConnection() {
        return new MongoConnectionDetails();
    }

    @Bean
    public MongoMappingContext mongoMappingContext() {
        return new MongoMappingContext();
    }

    @Bean
    public MongoOperations mongoTemplate(MongoConnectionDetails mongoConnectionDetails, MongoMappingContext mongoMappingContext)
            throws UnknownHostException, UnsupportedEncodingException {
        return getMongoTemplate(DUMMY_STATIC, mongoConnectionDetails, mongoMappingContext);
    }

}
