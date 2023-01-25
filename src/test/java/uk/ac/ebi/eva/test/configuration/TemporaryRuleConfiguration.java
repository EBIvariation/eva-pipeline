package uk.ac.ebi.eva.test.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;

@Configuration
public class TemporaryRuleConfiguration {

    @Value("${spring.data.mongodb.host}")
    String mongoHost;

    @Bean
    public TemporaryMongoRule temporaryMongoRule(){
        return new TemporaryMongoRule(mongoHost);
    }
}
