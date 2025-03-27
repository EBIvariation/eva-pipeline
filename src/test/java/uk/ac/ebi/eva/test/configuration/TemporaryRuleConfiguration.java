package uk.ac.ebi.eva.test.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;

@Configuration
public class TemporaryRuleConfiguration {

    @Value("${spring.data.mongodb.host}")
    String mongoHost;

    @Value("${spring.data.mongodb.port:#{null}}")
    Integer mongoPort;

    @Value("${spring.data.mongodb.uri:#{null}}")
    String mongoUri;

    @Bean
    @Scope(value="prototype")
    public TemporaryMongoRule temporaryMongoRule(){
        if (mongoUri != null && !mongoUri.isEmpty()) {
            return new TemporaryMongoRule(mongoUri);
        }
        return new TemporaryMongoRule(mongoHost, mongoPort);
    }
}
